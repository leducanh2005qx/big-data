import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Literal, Mapping

MetricType = Literal["aqi", "concentration"]


def build_observation_event(
    *,
    source: str,
    data_mode: str,
    source_record_id: str | None,
    station_id: str | None,
    country: str | None,
    city: str | None,
    latitude: float | None,
    longitude: float | None,
    observed_at: datetime | None,
    ingested_at: datetime | None,
    pollutant: str,
    metric_type: MetricType,
    value: float,
    unit: str,
    category: str | None,
    aqi_standard: str | None,
    quality_flags: list[str],
    attributions: list[dict[str, str | None]] | None = None,
) -> dict[str, object]:
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError("Observation value must be a finite, non-negative number")
    if not source.strip() or not pollutant.strip() or not unit.strip():
        raise ValueError("source, pollutant, and unit are required")
    if metric_type == "aqi" and unit != "AQI index":
        raise ValueError("AQI values must use the 'AQI index' unit")
    if metric_type == "concentration" and unit == "AQI index":
        raise ValueError("Concentration values cannot use the 'AQI index' unit")

    received_at = ingested_at or datetime.now(timezone.utc)
    if received_at.utcoffset() is None:
        raise ValueError("ingested_at must include a timezone")
    received_at = received_at.astimezone(timezone.utc)
    if observed_at is not None:
        if observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        observed_at = observed_at.astimezone(timezone.utc)

    for coordinate, lower, upper, label in (
        (latitude, -90, 90, "latitude"),
        (longitude, -180, 180, "longitude"),
    ):
        if coordinate is not None and (not math.isfinite(coordinate) or not lower <= coordinate <= upper):
            raise ValueError(f"{label} is outside its valid range")

    identity = {
        "source": source,
        "source_record_id": source_record_id,
        "station_id": station_id,
        "country": country,
        "city": city,
        "pollutant": pollutant,
        "metric_type": metric_type,
        "value": value,
        "unit": unit,
        "observed_at": observed_at.isoformat() if observed_at else None,
        "category": category,
        "aqi_standard": aqi_standard,
    }
    identity_json = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    event_id = hashlib.sha256(identity_json.encode("utf-8")).hexdigest()

    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "source": source,
        "data_mode": data_mode,
        "source_record_id": source_record_id,
        "station_id": station_id,
        "country": country,
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "observed_at": observed_at.isoformat() if observed_at else None,
        "ingested_at": received_at.isoformat(),
        "pollutant": pollutant,
        "metric_type": metric_type,
        "value": value,
        "unit": unit,
        "category": category,
        "aqi_standard": aqi_standard,
        "quality_flags": quality_flags,
        "attributions": attributions or [],
    }


def normalize_csv_aqi_row(
    row: Mapping[str, str | None],
    ingested_at: datetime | None = None,
) -> dict[str, object]:
    raw_value = row.get("PM2.5 AQI Value")
    if raw_value is None or not raw_value.strip():
        raise ValueError("PM2.5 AQI Value is missing")

    try:
        value = float(raw_value)
    except ValueError as error:
        raise ValueError("PM2.5 AQI Value must be numeric") from error

    if not math.isfinite(value) or value < 0:
        raise ValueError("PM2.5 AQI Value must be a finite, non-negative number")

    timestamp = ingested_at or datetime.now(timezone.utc)
    if timestamp.utcoffset() is None:
        raise ValueError("ingested_at must include a timezone")
    timestamp = timestamp.astimezone(timezone.utc)

    country = (row.get("Country") or "").strip() or None
    city = (row.get("City") or "").strip() or None
    category = (row.get("PM2.5 AQI Category") or "").strip() or None

    return build_observation_event(
        source="global_air_real_csv",
        data_mode="historical_csv_replay",
        source_record_id=None,
        station_id=None,
        country=country,
        city=city,
        latitude=None,
        longitude=None,
        observed_at=None,
        ingested_at=timestamp,
        pollutant="pm25",
        metric_type="aqi",
        value=value,
        unit="AQI index",
        category=category,
        aqi_standard=None,
        quality_flags=[
            "historical_snapshot",
            "missing_observed_at",
            "location_coordinates_unavailable",
        ],
    )