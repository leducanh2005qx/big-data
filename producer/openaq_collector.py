import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from dotenv import load_dotenv
from kafka import KafkaProducer

from .observation import build_observation_event

API_BASE_URL = "https://api.openaq.org/v3"
TOPIC_NAME = "air_quality_data"
STATUS_TOPIC_NAME = "air_quality_collection_status"
MAX_LOCATIONS_PER_RUN = 20
MAX_API_ATTEMPTS = 3
MAX_RETRY_DELAY_SECONDS = 5


def _parse_utc_timestamp(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("OpenAQ returned an invalid measurement timestamp") from error
    if timestamp.utcoffset() is None:
        raise ValueError("OpenAQ measurement timestamp has no timezone")
    return timestamp.astimezone(timezone.utc)


def normalize_openaq_latest(
    location: Mapping[str, Any],
    sensors_by_id: Mapping[str, Mapping[str, Any]],
    latest: Mapping[str, Any],
    ingested_at: datetime | None = None,
) -> dict[str, object]:
    sensor_id = str(latest.get("sensorsId", ""))
    sensor = sensors_by_id.get(sensor_id)
    if sensor is None:
        raise ValueError(f"OpenAQ location metadata is missing sensor {sensor_id}")

    parameter = sensor.get("parameter")
    if not isinstance(parameter, dict):
        raise ValueError(f"OpenAQ sensor {sensor_id} has no parameter metadata")

    pollutant = str(parameter.get("name", "")).strip().lower()
    unit = str(parameter.get("units", "")).strip()
    if not pollutant or not unit:
        raise ValueError(f"OpenAQ sensor {sensor_id} is missing pollutant or unit")

    date_value = latest.get("datetime")
    if not isinstance(date_value, dict) or not isinstance(date_value.get("utc"), str):
        raise ValueError("OpenAQ latest record is missing its UTC measurement timestamp")
    observed_at = _parse_utc_timestamp(date_value["utc"])

    try:
        value = float(latest["value"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("OpenAQ latest record has an invalid measurement value") from error

    latest_coordinates = latest.get("coordinates")
    location_coordinates = location.get("coordinates")
    coordinates = latest_coordinates if isinstance(latest_coordinates, dict) else location_coordinates
    if not isinstance(coordinates, dict):
        coordinates = {}

    country = location.get("country")
    country_code = country.get("code") if isinstance(country, dict) else None
    country_name = country.get("name") if isinstance(country, dict) else None
    quality_flags = []
    if coordinates.get("latitude") is None or coordinates.get("longitude") is None:
        quality_flags.append("location_coordinates_unavailable")

    return build_observation_event(
        source="openaq_v3",
        data_mode="measured",
        source_record_id=f"sensor:{sensor_id}:{observed_at.isoformat()}",
        station_id=str(location.get("id")) if location.get("id") is not None else None,
        country=str(country_code or country_name) if country_code or country_name else None,
        city=str(location["locality"]) if location.get("locality") else None,
        latitude=float(coordinates["latitude"]) if coordinates.get("latitude") is not None else None,
        longitude=float(coordinates["longitude"]) if coordinates.get("longitude") is not None else None,
        observed_at=observed_at,
        ingested_at=ingested_at,
        pollutant=pollutant,
        metric_type="concentration",
        value=value,
        unit=unit,
        category=None,
        aqi_standard=None,
        quality_flags=quality_flags,
        attributions=_location_attributions(location),
    )


def _location_attributions(location: Mapping[str, Any]) -> list[dict[str, str | None]]:
    attributions = [
        {"name": "OpenAQ", "url": "https://openaq.org", "license": "OpenAQ Terms of Use"}
    ]
    provider = location.get("provider")
    provider_name = provider.get("name") if isinstance(provider, dict) else None
    licenses = location.get("licenses")
    if not isinstance(licenses, list):
        return attributions

    for license_record in licenses:
        if not isinstance(license_record, dict):
            continue
        source_attribution = license_record.get("attribution")
        if not isinstance(source_attribution, dict):
            source_attribution = {}
        name = source_attribution.get("name") or provider_name or "Source provider"
        url = source_attribution.get("url")
        license_name = license_record.get("name") or "Unspecified source license"
        attributions.append({
            "name": str(name),
            "url": str(url) if url else None,
            "license": str(license_name),
        })
    return attributions


def build_collection_status_event(
    location_id: str,
    status: str,
    poll_started_at: datetime,
    poll_completed_at: datetime,
    records_published: int,
    records_rejected: int = 0,
    error: str | None = None,
) -> dict[str, object]:
    if status not in {"success", "partial", "failed"}:
        raise ValueError("Collection status must be success, partial, or failed")
    if records_published < 0 or records_rejected < 0:
        raise ValueError("Collection record counts cannot be negative")
    if poll_started_at.utcoffset() is None or poll_completed_at.utcoffset() is None:
        raise ValueError("Collection status timestamps must include a timezone")

    return {
        "event_type": "collection_status",
        "status_id": uuid4().hex,
        "source": "openaq_v3",
        "location_id": str(location_id),
        "status": status,
        "poll_started_at": poll_started_at.astimezone(timezone.utc).isoformat(),
        "poll_completed_at": poll_completed_at.astimezone(timezone.utc).isoformat(),
        "records_published": records_published,
        "records_rejected": records_rejected,
        "error": error[:500] if error else None,
    }


def _retry_delay(headers: Any, attempt: int) -> float:
    retry_after = headers.get("Retry-After") if headers else None
    try:
        delay = float(retry_after)
    except (TypeError, ValueError):
        delay = float(2 ** attempt)
    return min(max(delay, 0.0), MAX_RETRY_DELAY_SECONDS)


def fetch_json(
    path: str,
    api_key: str,
    timeout_seconds: int = 20,
    max_attempts: int = MAX_API_ATTEMPTS,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    request = Request(
        f"{API_BASE_URL}{path}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
    )
    for attempt in range(max_attempts):
        try:
            with opener(request, timeout=timeout_seconds) as response:
                payload = json.load(response)
            if not isinstance(payload, dict):
                raise RuntimeError("OpenAQ returned a non-object JSON response")
            return payload
        except HTTPError as error:
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable:
                raise RuntimeError(f"OpenAQ request failed with HTTP {error.code}") from error
            if attempt + 1 == max_attempts:
                raise RuntimeError(f"OpenAQ request failed after {max_attempts} attempts with HTTP {error.code}") from error
            delay = _retry_delay(error.headers, attempt)
            print(f"OpenAQ returned HTTP {error.code}; retrying in {delay:g}s")
            sleep(delay)
        except (URLError, TimeoutError) as error:
            if attempt + 1 == max_attempts:
                reason = getattr(error, "reason", str(error))
                raise RuntimeError(f"OpenAQ request failed after {max_attempts} attempts: {reason}") from error
            delay = min(float(2 ** attempt), MAX_RETRY_DELAY_SECONDS)
            print(f"OpenAQ connection failed; retrying in {delay:g}s")
            sleep(delay)

    raise RuntimeError("OpenAQ request failed without a response")


def _archive_response(
    archive_dir: Path,
    location_id: str,
    resource: str,
    payload: Mapping[str, Any],
    archived_at: datetime,
) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    timestamp = archived_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    filename = f"{timestamp}_location_{location_id}_{resource}.json"
    (archive_dir / filename).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _first_result(payload: Mapping[str, Any], resource: str) -> dict[str, Any]:
    results = payload.get("results")
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        raise RuntimeError(f"OpenAQ returned no {resource} result")
    return results[0]


def collect_location_events(
    location_id: str,
    api_key: str,
    fetch: Callable[[str, str], dict[str, Any]] = fetch_json,
    ingested_at: datetime | None = None,
    raw_archive_dir: Path | None = None,
    counters: dict[str, int] | None = None,
) -> list[dict[str, object]]:
    if not location_id.isdigit():
        raise ValueError("OpenAQ location IDs must be numeric")

    collection_time = ingested_at or datetime.now(timezone.utc)
    location = _first_result(fetch(f"/locations/{location_id}", api_key), "location")
    if raw_archive_dir is not None:
        _archive_response(raw_archive_dir, location_id, "location", {"results": [location]}, collection_time)
    latest_payload = fetch(f"/locations/{location_id}/latest", api_key)
    if raw_archive_dir is not None:
        _archive_response(raw_archive_dir, location_id, "latest", latest_payload, collection_time)
    latest_results = latest_payload.get("results")
    if not isinstance(latest_results, list):
        raise RuntimeError("OpenAQ latest response is missing results")

    sensors = location.get("sensors")
    if not isinstance(sensors, list):
        raise RuntimeError("OpenAQ location response is missing sensor metadata")
    sensors_by_id = {
        str(sensor["id"]): sensor
        for sensor in sensors
        if isinstance(sensor, dict) and sensor.get("id") is not None
    }

    events = []
    rejected = 0
    for latest in latest_results:
        if not isinstance(latest, dict):
            rejected += 1
            continue
        try:
            events.append(normalize_openaq_latest(location, sensors_by_id, latest, collection_time))
        except ValueError as error:
            rejected += 1
            print(f"Skipping invalid OpenAQ latest record for location {location_id}: {error}")
    if rejected:
        print(f"Rejected {rejected} invalid OpenAQ records for location {location_id}")
    if counters is not None:
        counters["records_rejected"] = rejected
    return events


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    api_key = os.getenv("OPENAQ_API_KEY", "").strip()
    location_ids = [value.strip() for value in os.getenv("OPENAQ_LOCATION_IDS", "").split(",") if value.strip()]
    if not api_key:
        raise RuntimeError("Set OPENAQ_API_KEY in the project .env file")
    if not location_ids:
        raise RuntimeError("Set one or more OPENAQ_LOCATION_IDS in the project .env file")
    if len(location_ids) > MAX_LOCATIONS_PER_RUN:
        raise RuntimeError(f"Limit each run to {MAX_LOCATIONS_PER_RUN} OpenAQ locations")

    archive_enabled = os.getenv("OPENAQ_RAW_ARCHIVE", "true").strip().lower() in {"1", "true", "yes"}
    raw_archive_dir = project_root / "data" / "raw" / "openaq" if archive_enabled else None

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    sent = 0
    failed_locations = 0
    try:
        for location_id in location_ids:
            poll_started_at = datetime.now(timezone.utc)
            counters: dict[str, int] = {}
            try:
                events = collect_location_events(
                    location_id,
                    api_key,
                    raw_archive_dir=raw_archive_dir,
                    counters=counters,
                )
            except (RuntimeError, ValueError) as error:
                failed_locations += 1
                print(f"OpenAQ collection failed for location {location_id}: {error}")
                producer.send(STATUS_TOPIC_NAME, value=build_collection_status_event(
                    location_id=location_id,
                    status="failed",
                    poll_started_at=poll_started_at,
                    poll_completed_at=datetime.now(timezone.utc),
                    records_published=0,
                    error=str(error),
                )).get(timeout=30)
                continue
            published_for_location = 0
            for event in events:
                producer.send(TOPIC_NAME, value=event).get(timeout=30)
                sent += 1
                published_for_location += 1
            rejected_for_location = counters.get("records_rejected", 0)
            status = "partial" if rejected_for_location else "success"
            producer.send(STATUS_TOPIC_NAME, value=build_collection_status_event(
                location_id=location_id,
                status=status,
                poll_started_at=poll_started_at,
                poll_completed_at=datetime.now(timezone.utc),
                records_published=published_for_location,
                records_rejected=rejected_for_location,
            )).get(timeout=30)
    finally:
        producer.close()

    print(
        f"OpenAQ run finished: published={sent}, "
        f"failed_locations={failed_locations}, configured_locations={len(location_ids)}"
    )
    if failed_locations == len(location_ids):
        raise RuntimeError("OpenAQ collection failed for every configured location")


if __name__ == "__main__":
    main()