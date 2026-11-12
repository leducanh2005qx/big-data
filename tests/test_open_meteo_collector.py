from datetime import datetime, timezone

import pytest

from producer.open_meteo_collector import (
    build_collection_status,
    normalize_current_response,
    normalize_open_meteo_response,
    parse_locations,
)


def test_parse_locations_and_normalize_model_records():
    location = parse_locations("hanoi|VN|Hanoi|21.0285|105.8542")[0]
    payload = {
        "hourly": {
            "time": ["2026-10-19T00:00", "2026-10-19T01:00"],
            "pm2_5": [12.5, 13.0],
            "pm10": [20.0, None],
        }
    }
    events = normalize_open_meteo_response(
        location, payload, datetime(2026, 10, 19, 2, tzinfo=timezone.utc)
    )
    assert len(events) == 3
    assert {event["source"] for event in events} == {"open_meteo_air_quality"}
    assert {event["data_mode"] for event in events} == {"model_estimate"}
    assert {event["unit"] for event in events} == {"ug/m3"}
    assert events[0]["observed_at"].endswith("+00:00")


def test_parse_locations_rejects_bad_shape():
    with pytest.raises(ValueError, match=r"id\|country\|city"):
        parse_locations("hanoi|VN|21.0|105.8")


def test_parse_locations_rejects_coordinates_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        parse_locations("hanoi|VN|Hanoi|91|105.8")


def test_collection_status_preserves_source_and_counts():
    timestamp = datetime(2026, 10, 19, tzinfo=timezone.utc)
    status = build_collection_status("hanoi", "partial", timestamp, timestamp, 2, 1, "bad row")
    assert status["source"] == "open_meteo_air_quality"
    assert status["records_published"] == 2
    assert status["records_rejected"] == 1


def test_normalize_current_response_keeps_aqi_standards_distinct():
    location = parse_locations("hanoi|VN|Hanoi|21.0285|105.8542")[0]
    events = normalize_current_response(
        location,
        {"current": {"time": "2026-10-19T02:00", "us_aqi": 42, "european_aqi": 18}},
        datetime(2026, 10, 19, 2, 1, tzinfo=timezone.utc),
    )
    assert {event["pollutant"] for event in events} == {"us_aqi", "european_aqi"}
    assert {event["metric_type"] for event in events} == {"aqi"}
    assert {event["unit"] for event in events} == {"AQI index"}
    assert {event["aqi_standard"] for event in events} == {"US-AQI", "European-AQI"}
