"""Forecasting, anomaly detection, recommendations and email alerts.

The module deliberately uses transparent, bounded statistics instead of hiding
the result behind an opaque model. Forecasts are labelled as estimates and all
thresholds are configurable through environment variables.
"""

from __future__ import annotations

import base64
import json
import os
import smtplib
import statistics
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


CITY_LABELS = {
    "Hanoi": "Hà Nội", "Hai Phong": "Hải Phòng", "Ha Long": "Hạ Long", "Vinh": "Vinh", "Hue": "Huế",
    "Ho Chi Minh City": "TP. Hồ Chí Minh", "Da Nang": "Đà Nẵng", "Quy Nhon": "Quy Nhơn",
    "Nha Trang": "Nha Trang", "Da Lat": "Đà Lạt", "Bien Hoa": "Biên Hòa", "Can Tho": "Cần Thơ",
    "Buon Ma Thuot": "Buôn Ma Thuột",
    "Cao Bang": "Cao Bằng", "Tuyen Quang": "Tuyên Quang", "Dien Bien": "Điện Biên", "Lai Chau": "Lai Châu",
    "Son La": "Sơn La", "Lao Cai": "Lào Cai", "Thai Nguyen": "Thái Nguyên", "Lang Son": "Lạng Sơn",
    "Quang Ninh": "Quảng Ninh", "Bac Ninh": "Bắc Ninh", "Phu Tho": "Phú Thọ", "Hung Yen": "Hưng Yên",
    "Ninh Binh": "Ninh Bình", "Thanh Hoa": "Thanh Hóa", "Nghe An": "Nghệ An", "Ha Tinh": "Hà Tĩnh",
    "Quang Tri": "Quảng Trị", "Quang Ngai": "Quảng Ngãi", "Gia Lai": "Gia Lai", "Khanh Hoa": "Khánh Hòa",
    "Dak Lak": "Đắk Lắk", "Lam Dong": "Lâm Đồng", "Dong Nai": "Đồng Nai", "Tay Ninh": "Tây Ninh",
    "Dong Thap": "Đồng Tháp", "Vinh Long": "Vĩnh Long", "An Giang": "An Giang", "Ca Mau": "Cà Mau",
}


def _pollutant_label(value: str) -> str:
    return {"us_aqi": "AQI", "pm25": "PM2.5", "pm10": "PM10"}.get(value, value.upper())


def _config() -> tuple[str, str, str, str, str]:
    return (
        os.getenv("INFLUXDB_HOST", "influxdb"),
        os.getenv("INFLUXDB_PORT", "8086"),
        os.getenv("INFLUXDB_DATABASE", "air_quality"),
        os.getenv("INFLUXDB_ADMIN_USER", ""),
        os.getenv("INFLUXDB_ADMIN_PASSWORD", ""),
    )


def influx_query(query: str) -> dict[str, object]:
    host, port, database, user, password = _config()
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = Request(
        f"http://{host}:{port}/query",
        data=urlencode({"db": database, "q": query}).encode("utf-8"),
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.load(response)
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"InfluxDB query failed: {detail or error}") from error
    error = payload.get("results", [{}])[0].get("error")
    if error:
        raise RuntimeError(f"InfluxDB query failed: {error}")
    return payload


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _number(value: float) -> float | int:
    return int(value) if value.is_integer() else round(value, 2)


def _parse_time(value: object) -> datetime:
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _category(value: float) -> str:
    if value <= 50:
        return "Tốt"
    if value <= 100:
        return "Trung bình"
    if value <= 150:
        return "Không tốt cho nhóm nhạy cảm"
    if value <= 200:
        return "Không tốt"
    if value <= 300:
        return "Rất không tốt"
    return "Nguy hại"


def _rows_from_payload(payload: dict[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    response = payload.get("results", [{}])[0]
    for series in response.get("series", []):
        tags = series.get("tags", {})
        columns = series.get("columns", [])
        if "time" not in columns or "value" not in columns:
            continue
        time_index, value_index = columns.index("time"), columns.index("value")
        for values in series.get("values", []):
            if len(values) <= max(time_index, value_index) or values[value_index] is None:
                continue
            try:
                rows.append({**tags, "time": values[time_index], "value": float(values[value_index])})
            except (TypeError, ValueError):
                continue
    return rows


def query_metric_rows(hours: int = 168, city: str | None = None,
                      pollutants: tuple[str, ...] = ("us_aqi", "pm25", "pm10")) -> list[dict[str, object]]:
    clauses = [f"time > now() - {max(1, min(168, int(hours)))}h", "time <= now()"]
    if city:
        clauses.append(f'"city" = {_literal(city)}')
    clauses.append("(" + " OR ".join(f'"pollutant" = {_literal(item)}' for item in pollutants) + ")")
    query = (
        'SELECT "value" FROM "raw_7d"."air_quality_observation" WHERE '
        + " AND ".join(clauses)
        + ' GROUP BY "city","pollutant","metric_type","source","data_mode","aqi_standard"'
    )
    return _rows_from_payload(influx_query(query))


def _hourly(rows: list[dict[str, object]]) -> dict[tuple[str, str], list[dict[str, object]]]:
    buckets: dict[tuple[str, str, int], list[float]] = {}
    for row in rows:
        try:
            timestamp = _parse_time(row["time"])
            key = (str(row.get("city") or "Unknown"), str(row.get("pollutant") or "unknown"), int(timestamp.timestamp() // 3600))
            buckets.setdefault(key, []).append(float(row["value"]))
        except (KeyError, TypeError, ValueError):
            continue
    result: dict[tuple[str, str], list[dict[str, object]]] = {}
    for (city, pollutant, hour), values in buckets.items():
        result.setdefault((city, pollutant), []).append({
            "time": datetime.fromtimestamp(hour * 3600, tz=timezone.utc),
            "value": sum(values) / len(values),
        })
    for values in result.values():
        values.sort(key=lambda item: item["time"])
    return result


def forecast_aqi(city: str | None = None, horizon: int = 24, history_hours: int = 168) -> dict[str, object]:
    horizon = max(1, min(48, int(horizon)))
    series = _hourly(query_metric_rows(history_hours, city, ("us_aqi",)))
    output: dict[str, object] = {
        "method": "xu hướng tuyến tính giảm dần",
        "is_estimate": True,
        "horizon_hours": horizon,
        "generated_at": _iso(datetime.now(timezone.utc)),
        "cities": {},
    }
    for (city_name, pollutant), values in series.items():
        if pollutant != "us_aqi" or not values:
            continue
        recent = values[-min(12, len(values)):]
        current = float(recent[-1]["value"])
        if len(recent) >= 3:
            slope = (float(recent[-1]["value"]) - float(recent[0]["value"])) / max(1, len(recent) - 1)
        else:
            slope = 0.0
        forecast: list[dict[str, object]] = []
        for step in range(1, horizon + 1):
            # Damping prevents a short spike from growing without bound.
            value = max(0.0, min(500.0, current + slope * step * (0.85 ** (step / 6))))
            timestamp = recent[-1]["time"] + timedelta(hours=step)
            forecast.append({"time": _iso(timestamp), "aqi": _number(value), "category": _category(value)})
        output["cities"][city_name] = {
            "city": city_name,
            "label": CITY_LABELS.get(city_name, city_name),
            "current_aqi": _number(current),
            "current_category": _category(current),
            "trend_per_hour": _number(slope),
            "last_observation": _iso(recent[-1]["time"]),
            "forecast": forecast,
        }
    return output


def detect_anomalies(hours: int = 168, city: str | None = None, robust_threshold: float = 3.5) -> dict[str, object]:
    grouped = _hourly(query_metric_rows(hours, city, ("us_aqi", "pm25", "pm10")))
    anomalies: list[dict[str, object]] = []
    for (city, pollutant), values in grouped.items():
        if len(values) < 6:
            continue
        latest = float(values[-1]["value"])
        baseline = [float(item["value"]) for item in values[:-1][-24:]]
        median = statistics.median(baseline)
        deviations = [abs(item - median) for item in baseline]
        mad = statistics.median(deviations)
        scale = mad * 1.4826 if mad else (statistics.pstdev(baseline) or 1.0)
        score = abs(latest - median) / scale
        minimum_change = 5.0 if pollutant == "us_aqi" else 3.0
        if score >= robust_threshold and abs(latest - median) >= minimum_change:
            anomalies.append({
                "city": city,
                "label": CITY_LABELS.get(city, city),
                "pollutant": pollutant,
                "value": _number(latest),
                "baseline": _number(median),
                "robust_score": _number(score),
                "direction": "tăng đột biến" if latest > median else "giảm bất thường",
                "time": _iso(values[-1]["time"]),
            })
    anomalies.sort(key=lambda item: float(item["robust_score"]), reverse=True)
    return {
        "generated_at": _iso(datetime.now(timezone.utc)),
        "method": "median-MAD theo chuỗi dữ liệu theo giờ",
        "threshold": robust_threshold,
        "anomalies": anomalies,
        "status": "Phát hiện bất thường" if anomalies else "Chưa phát hiện bất thường",
    }


def recommend_cities(hours: int = 24, city: str | None = None, limit: int = 5) -> dict[str, object]:
    latest: dict[tuple[str, str], dict[str, object]] = {}
    for row in query_metric_rows(hours, city, ("us_aqi", "pm25", "pm10")):
        key = (str(row.get("city") or "Unknown"), str(row.get("pollutant") or "unknown"))
        if key not in latest or str(row.get("time", "")) > str(latest[key].get("time", "")):
            latest[key] = row
    by_city: dict[str, dict[str, float]] = {}
    times: dict[str, str] = {}
    for (city, pollutant), row in latest.items():
        by_city.setdefault(city, {})[pollutant] = float(row["value"])
        times[city] = max(times.get(city, ""), str(row.get("time", "")))
    rankings: list[dict[str, object]] = []
    for city, metrics in by_city.items():
        weights = (("us_aqi", 0.60, 150.0), ("pm25", 0.25, 75.0), ("pm10", 0.15, 150.0))
        available = [(value, weight, cap) for pollutant, weight, cap in weights if (value := metrics.get(pollutant)) is not None]
        if not available:
            continue
        total_weight = sum(item[1] for item in available)
        pollution_score = sum(min(value / cap, 1.0) * weight for value, weight, cap in available) / total_weight
        score = max(0.0, min(100.0, 100.0 * (1.0 - pollution_score)))
        rankings.append({
            "city": city,
            "label": CITY_LABELS.get(city, city),
            "score": _number(score),
            "aqi": _number(metrics["us_aqi"]) if "us_aqi" in metrics else None,
            "pm25": _number(metrics["pm25"]) if "pm25" in metrics else None,
            "pm10": _number(metrics["pm10"]) if "pm10" in metrics else None,
            "last_observation": times.get(city),
            "reason": "Điểm thấp hơn dựa trên AQI và nồng độ bụi hiện có; đây là gợi ý theo dữ liệu, không phải tư vấn y tế.",
        })
    rankings.sort(key=lambda item: float(item["score"]), reverse=True)
    return {
        "generated_at": _iso(datetime.now(timezone.utc)),
        "time_range": f"{hours}h",
        "method": "điểm sạch tương đối: AQI 60%, PM2.5 25%, PM10 15% (chuẩn hóa theo ngưỡng cấu hình)",
        "recommendations": rankings[:max(1, min(20, int(limit)))],
    }


def collect_alerts(hours: int = 24) -> list[dict[str, object]]:
    thresholds = {
        "us_aqi": float(os.getenv("ALERT_AQI_THRESHOLD", "100")),
        "pm25": float(os.getenv("ALERT_PM25_THRESHOLD", "35")),
        "pm10": float(os.getenv("ALERT_PM10_THRESHOLD", "150")),
    }
    latest: dict[tuple[str, str], dict[str, object]] = {}
    for row in query_metric_rows(hours, None, tuple(thresholds)):
        key = (str(row.get("city") or "Unknown"), str(row.get("pollutant") or "unknown"))
        if key not in latest or str(row.get("time", "")) > str(latest[key].get("time", "")):
            latest[key] = row
    alerts: list[dict[str, object]] = []
    for (city, pollutant), row in latest.items():
        value = float(row["value"])
        threshold = thresholds[pollutant]
        if value >= threshold:
            alerts.append({
                "city": city,
                "label": CITY_LABELS.get(city, city),
                "pollutant": pollutant,
                "kind": "threshold",
                "value": _number(value),
                "threshold": threshold,
                "time": row.get("time"),
                "message": f"{CITY_LABELS.get(city, city)} có {_pollutant_label(pollutant)} {value:g}, vượt ngưỡng cấu hình {threshold:g}.",
            })
    # Alerts are event-driven: a forecast is emailed only when it predicts an
    # AQI threshold crossing, not on every periodic forecast refresh.
    try:
        anomalies = detect_anomalies(max(24, hours)).get("anomalies", [])
        for item in anomalies:
            alerts.append({
                "city": item["city"],
                "label": item["label"],
                "pollutant": item["pollutant"],
                "kind": "anomaly",
                "value": item["value"],
                "threshold": None,
                "time": item["time"],
                "message": f"{item['label']} có {_pollutant_label(str(item['pollutant']))} {item['value']}, {item['direction']} so với mức nền.",
            })
    except Exception:
        # Current threshold alerts remain available if the anomaly query fails.
        pass
    try:
        forecast = forecast_aqi(horizon=24, history_hours=max(24, hours)).get("cities", {})
        aqi_threshold = thresholds["us_aqi"]
        current_alert_cities = {item["city"] for item in alerts if item.get("kind") == "threshold" and item.get("pollutant") == "us_aqi"}
        for city, item in forecast.items():
            if city in current_alert_cities:
                continue
            crossing = next((point for point in item.get("forecast", []) if float(point["aqi"]) >= aqi_threshold), None)
            if crossing:
                alerts.append({
                    "city": city,
                    "label": item["label"],
                    "pollutant": "us_aqi",
                    "kind": "forecast",
                    "value": crossing["aqi"],
                    "threshold": aqi_threshold,
                    "time": crossing["time"],
                    "message": f"Dự báo {item['label']} có AQI khoảng {crossing['aqi']}, có thể vượt ngưỡng cấu hình {aqi_threshold:g} vào {crossing['time']}.",
                })
    except Exception:
        pass
    return alerts


def email_status() -> dict[str, object]:
    recipients = [item.strip() for item in os.getenv("ALERT_EMAIL_TO", "").split(",") if item.strip()]
    return {
        "enabled": os.getenv("ALERT_EMAIL_ENABLED", "false").lower() in {"1", "true", "yes"},
        "dry_run": os.getenv("ALERT_EMAIL_DRY_RUN", "true").lower() in {"1", "true", "yes"},
        "smtp_configured": bool(os.getenv("SMTP_HOST", "").strip() and os.getenv("SMTP_USERNAME", "").strip() and os.getenv("SMTP_PASSWORD", "").strip()),
        "sender": os.getenv("ALERT_EMAIL_FROM", "") or os.getenv("SMTP_USERNAME", ""),
        "recipient_count": len(recipients),
    }


def build_email_preview(alerts: list[dict[str, object]]) -> dict[str, object]:
    subject = f"[AirQuality] Có {len(alerts)} cảnh báo chất lượng không khí"
    body = "CẢNH BÁO CHẤT LƯỢNG KHÔNG KHÍ\n\n" + "\n".join(f"- {alert['message']} Thời điểm: {alert['time']}" for alert in alerts)
    body += "\n\nNgưỡng do người vận hành cấu hình; cần đối chiếu chuẩn AQI và nguồn đo trước khi sử dụng cho quyết định sức khỏe."
    return {"subject": subject, "body": body, "alert_count": len(alerts)}


def build_demo_alerts(value: float | None = None) -> list[dict[str, object]]:
    demo_value = float(value if value is not None else os.getenv("AQI_ALERT_TEST_VALUE", "151"))
    return [{
        "city": "Demo Presentation",
        "label": "Hà Nội (DEMO)",
        "pollutant": "us_aqi",
        "kind": "demo",
        "value": _number(demo_value),
        "threshold": float(os.getenv("ALERT_AQI_THRESHOLD", "100")),
        "time": _iso(datetime.now(timezone.utc)),
        "message": f"[DEMO] Hà Nội có AQI {demo_value:g}, vượt ngưỡng trình diễn. Đây không phải dữ liệu quan trắc thật.",
    }]


def send_alert_email(alerts: list[dict[str, object]]) -> dict[str, object]:
    status = email_status()
    if not alerts:
        return {"sent": False, "reason": "no_alerts", **status}
    preview = build_email_preview(alerts)
    subject, body = preview["subject"], preview["body"]
    if not status["enabled"]:
        return {"sent": False, "reason": "email_disabled", "preview": body, **status}
    if status["dry_run"]:
        return {"sent": False, "reason": "dry_run", "preview": body, **status}
    host = os.getenv("SMTP_HOST", "").strip()
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "")
    recipients = [item.strip() for item in os.getenv("ALERT_EMAIL_TO", "").split(",") if item.strip()]
    if not host or not username or not password or not recipients:
        return {"sent": False, "reason": "smtp_or_recipient_missing", **status}
    message = EmailMessage()
    message["Subject"], message["From"], message["To"] = subject, os.getenv("ALERT_EMAIL_FROM", "").strip() or username, ", ".join(recipients)
    message.set_content(body)
    port = int(os.getenv("SMTP_PORT", "587"))
    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=20) as server:
            server.login(username, password)
            server.send_message(message)
    else:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(username, password)
            server.send_message(message)
    return {"sent": True, "recipient_count": len(recipients), "sender": message["From"]}
