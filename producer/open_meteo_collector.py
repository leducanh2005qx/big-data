"""Open-Meteo air-quality adapter.

Open-Meteo is the second source used by Phase 7. Its records are model
estimates, not station measurements, so the canonical event uses
``data_mode=model_estimate`` and preserves the source attribution.
"""

import json
import os
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from kafka import KafkaProducer

from .observation import build_observation_event

API_BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
TOPIC_NAME = "air_quality_data"
STATUS_TOPIC_NAME = "air_quality_collection_status"
MAX_API_ATTEMPTS = 3
MAX_RETRY_DELAY_SECONDS = 5

VARIABLES = {
    "pm2_5": ("pm25", "ug/m3"),
    "pm10": ("pm10", "ug/m3"),
    "carbon_monoxide": ("co", "ug/m3"),
    "nitrogen_dioxide": ("no2", "ug/m3"),
    "sulphur_dioxide": ("so2", "ug/m3"),
    "ozone": ("o3", "ug/m3"),
}
AQI_VARIABLES = {
    "us_aqi": ("us_aqi", "AQI index", "US-AQI"),
    "european_aqi": ("european_aqi", "AQI index", "European-AQI"),
}


def parse_locations(value: str) -> list[dict[str, object]]:
    """Parse ``id|country|city|latitude|longitude;...`` configuration."""
    locations = []
    for item in value.split(";"):
        item = item.strip()
        if not item:
            continue
        parts = [part.strip() for part in item.split("|")]
        if len(parts) != 5:
            raise ValueError("OPENMETEO_LOCATIONS must use id|country|city|latitude|longitude")
        location_id, country, city, latitude, longitude = parts
        try:
            lat = float(latitude)
            lon = float(longitude)
        except ValueError as error:
            raise ValueError(f"Invalid Open-Meteo coordinates for {location_id}") from error
        if not location_id or not city:
            raise ValueError("Open-Meteo location id and city are required")
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            raise ValueError(f"Open-Meteo coordinates out of range for {location_id}")
        locations.append({"id": location_id, "country": country or None, "city": city,
                          "latitude": lat, "longitude": lon})
    if not locations:
        raise ValueError("Set at least one Open-Meteo location")
    return locations


def _parse_time(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def normalize_open_meteo_response(
    location: Mapping[str, object],
    payload: Mapping[str, Any],
    ingested_at: datetime | None = None,
    reference_time: datetime | None = None,
) -> list[dict[str, object]]:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        raise ValueError("Open-Meteo response is missing hourly time values")
    times = hourly["time"]
    events = []
    for variable, (pollutant, unit) in VARIABLES.items():
        values = hourly.get(variable)
        if not isinstance(values, list):
            continue
        for observed_text, raw_value in zip(times, values):
            if not isinstance(observed_text, str) or raw_value is None:
                continue
            try:
                observed_at = _parse_time(observed_text)
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if reference_time is not None and observed_at > reference_time:
                continue
            try:
                events.append(build_observation_event(
                    source="open_meteo_air_quality",
                    data_mode="model_estimate",
                    source_record_id=f"{location['id']}:{observed_at.isoformat()}:{pollutant}",
                    station_id=None,
                    country=str(location.get("country")) if location.get("country") else None,
                    city=str(location["city"]),
                    latitude=float(location["latitude"]),
                    longitude=float(location["longitude"]),
                    observed_at=observed_at,
                    ingested_at=ingested_at,
                    pollutant=pollutant,
                    metric_type="concentration",
                    value=value,
                    unit=unit,
                    category=None,
                    aqi_standard=None,
                    quality_flags=["model_estimate", "grid_cell_value"],
                    attributions=[
                        {"name": "Open-Meteo", "url": "https://open-meteo.com/", "license": "Open-Meteo attribution"},
                        {"name": "CAMS ENSEMBLE", "url": "https://atmosphere.copernicus.eu/", "license": "CAMS attribution"},
                    ],
                ))
            except ValueError:
                # One invalid model value must not discard the other variables/times.
                continue
    return events


def normalize_current_response(
    location: Mapping[str, object],
    payload: Mapping[str, Any],
    ingested_at: datetime | None = None,
) -> list[dict[str, object]]:
    """Normalize the provider's current snapshot without treating forecasts as observations."""
    current = payload.get("current")
    if not isinstance(current, dict) or not isinstance(current.get("time"), str):
        return []
    observed_at = _parse_time(current["time"])
    events = []
    for variable, (pollutant, unit, standard) in AQI_VARIABLES.items():
        raw_value = current.get(variable)
        if raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        try:
            events.append(build_observation_event(
                source="open_meteo_air_quality",
                data_mode="model_estimate",
                source_record_id=f"{location['id']}:{observed_at.isoformat()}:{pollutant}",
                station_id=None,
                country=str(location.get("country")) if location.get("country") else None,
                city=str(location["city"]),
                latitude=float(location["latitude"]),
                longitude=float(location["longitude"]),
                observed_at=observed_at,
                ingested_at=ingested_at,
                pollutant=pollutant,
                metric_type="aqi",
                value=value,
                unit=unit,
                category=None,
                aqi_standard=standard,
                quality_flags=["model_estimate", "current_snapshot"],
                attributions=[
                    {"name": "Open-Meteo", "url": "https://open-meteo.com/", "license": "Open-Meteo attribution"},
                    {"name": "CAMS ENSEMBLE", "url": "https://atmosphere.copernicus.eu/", "license": "CAMS attribution"},
                ],
            ))
        except ValueError:
            continue
    return events


def fetch_json(
    location: Mapping[str, object],
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    max_attempts: int = MAX_API_ATTEMPTS,
) -> dict[str, Any]:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": ",".join([*VARIABLES, *AQI_VARIABLES]),
        "hourly": ",".join(VARIABLES),
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    request = Request(f"{API_BASE_URL}?{urlencode(params)}", headers={"Accept": "application/json"})
    for attempt in range(max_attempts):
        try:
            with opener(request, timeout=20) as response:
                payload = json.load(response)
            if not isinstance(payload, dict):
                raise RuntimeError("Open-Meteo returned a non-object JSON response")
            return payload
        except HTTPError as error:
            if error.code < 500 and error.code != 429:
                raise RuntimeError(f"Open-Meteo request failed with HTTP {error.code}") from error
            if attempt + 1 == max_attempts:
                raise RuntimeError(f"Open-Meteo request failed after {max_attempts} attempts") from error
        except (URLError, TimeoutError) as error:
            if attempt + 1 == max_attempts:
                raise RuntimeError("Open-Meteo request failed after retries") from error
        sleep(min(2 ** attempt, MAX_RETRY_DELAY_SECONDS))
    raise RuntimeError("Open-Meteo request failed without a response")


def build_collection_status(location_id: str, status: str, started: datetime,
                            completed: datetime, published: int, rejected: int = 0,
                            error: str | None = None) -> dict[str, object]:
    return {
        "event_type": "collection_status",
        "status_id": f"open-meteo-{location_id}-{completed.timestamp()}",
        "source": "open_meteo_air_quality",
        "location_id": str(location_id),
        "status": status,
        "poll_started_at": started.astimezone(timezone.utc).isoformat(),
        "poll_completed_at": completed.astimezone(timezone.utc).isoformat(),
        "records_published": published,
        "records_rejected": rejected,
        "error": error[:500] if error else None,
    }


def _load_seen_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _remember_ids(path: Path, event_ids: list[str]) -> None:
    if not event_ids:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        for event_id in event_ids:
            stream.write(event_id + "\n")


def collect_once(producer: KafkaProducer, locations: list[dict[str, object]], dedup_path: Path) -> dict[str, int]:
    seen_ids = _load_seen_ids(dedup_path)
    totals = {"requests": 0, "published": 0, "duplicates": 0, "rejected": 0, "failed": 0}
    for location in locations:
        started = datetime.now(timezone.utc)
        location_rejected = 0
        try:
            totals["requests"] += 1
            payload = fetch_json(location)
            events = normalize_current_response(location, payload, started)
            events.extend(normalize_open_meteo_response(location, payload, started, started))
            unique_events = [event for event in events if event["source_record_id"] not in seen_ids]
            totals["duplicates"] += len(events) - len(unique_events)
            for event in unique_events:
                producer.send(TOPIC_NAME, value=event).get(timeout=30)
            ids = [str(event["source_record_id"]) for event in unique_events]
            seen_ids.update(ids)
            _remember_ids(dedup_path, ids)
            totals["published"] += len(unique_events)
            status = "success" if unique_events else "partial"
            error = None if unique_events else "No new current or historical values returned"
        except (RuntimeError, ValueError) as error:
            totals["failed"] += 1
            totals["rejected"] += 1
            location_rejected = 1
            unique_events = []
            status = "failed"
            print(f"Open-Meteo collection failed for {location['id']}: {error}")
        producer.send(STATUS_TOPIC_NAME, value=build_collection_status(
            str(location["id"]), status, started, datetime.now(timezone.utc),
            len(unique_events), location_rejected, str(error) if status == "failed" else None,
        )).get(timeout=30)
        print(
            f"Open-Meteo location={location['id']} published={len(unique_events)} "
            f"duplicates={totals['duplicates']} status={status}"
        )
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Open-Meteo air-quality data into Kafka")
    parser.add_argument("--once", action="store_true", help="poll once and exit (default)")
    parser.add_argument("--continuous", action="store_true", help="poll repeatedly")
    parser.add_argument("--interval-seconds", type=int, default=None)
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    try:
        locations = parse_locations(os.getenv("OPENMETEO_LOCATIONS", ""))
    except ValueError as error:
        raise SystemExit(
            f"Open-Meteo configuration error: {error}. "
            "Set OPENMETEO_LOCATIONS in .env using "
            "id|country|city|latitude|longitude;..."
        ) from None
    if args.once and args.continuous:
        raise SystemExit("Choose only one of --once or --continuous")
    interval = args.interval_seconds or int(os.getenv("OPENMETEO_INTERVAL_SECONDS", "600"))
    if interval < 300:
        raise SystemExit("OPENMETEO_INTERVAL_SECONDS must be at least 300 seconds")
    dedup_path = project_root / os.getenv("OPENMETEO_DEDUP_FILE", "data/state/open_meteo_seen_ids.txt")
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        while True:
            totals = collect_once(producer, locations, dedup_path)
            print(f"Open-Meteo run finished: {totals}")
            if not args.continuous:
                break
            print(f"Sleeping {interval}s before next Open-Meteo poll")
            time.sleep(interval)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
