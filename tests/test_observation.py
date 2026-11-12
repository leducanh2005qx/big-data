import json
from pathlib import Path
import unittest
from datetime import datetime, timezone

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from producer.openaq_collector import collect_location_events, normalize_openaq_latest
from producer.observation import normalize_csv_aqi_row
from producer.send_synthetic_alert_test import build_synthetic_alert_event


class NormalizeCsvAqiRowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "air_quality_observation.schema.json"
        with schema_path.open(encoding="utf-8") as schema_file:
            cls.schema = json.load(schema_file)
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(cls.schema, format_checker=FormatChecker())

    def setUp(self):
        self.row = {
            "Country": "Vietnam",
            "City": "Hanoi",
            "PM2.5 AQI Value": "51",
            "PM2.5 AQI Category": "Moderate",
        }
        self.ingested_at = datetime(2026, 10, 17, 8, 30, tzinfo=timezone.utc)

    def test_maps_aqi_without_claiming_concentration_or_observation_time(self):
        event = normalize_csv_aqi_row(self.row, self.ingested_at)

        self.assertEqual(event["metric_type"], "aqi")
        self.assertEqual(event["pollutant"], "pm25")
        self.assertEqual(event["value"], 51.0)
        self.assertEqual(event["unit"], "AQI index")
        self.assertIsNone(event["aqi_standard"])
        self.assertIsNone(event["observed_at"])
        self.assertEqual(event["ingested_at"], self.ingested_at.isoformat())
        self.assertEqual(event["data_mode"], "historical_csv_replay")
        self.validator.validate(event)

    def test_schema_rejects_aqi_index_as_concentration_unit(self):
        event = normalize_csv_aqi_row(self.row, self.ingested_at)
        event["metric_type"] = "concentration"

        with self.assertRaises(ValidationError):
            self.validator.validate(event)

    def test_schema_rejects_invalid_timestamp_and_missing_required_field(self):
        event = normalize_csv_aqi_row(self.row, self.ingested_at)
        event["ingested_at"] = "not-a-timestamp"

        with self.assertRaises(ValidationError):
            self.validator.validate(event)

    def test_maps_openaq_sensor_measurement_to_concentration_event(self):
        location = {
            "id": 2178,
            "locality": "Albuquerque",
            "country": {"code": "US", "name": "United States"},
            "coordinates": {"latitude": 35.1353, "longitude": -106.584702},
            "provider": {"name": "AirNow"},
            "licenses": [
                {
                    "name": "US Public Domain",
                    "attribution": {"name": "AirNow", "url": "https://www.airnow.gov/"},
                }
            ],
        }
        sensors_by_id = {
            "3920": {
                "id": 3920,
                "parameter": {"name": "pm25", "units": "µg/m³"},
            }
        }
        latest = {
            "datetime": {"utc": "2026-10-17T08:00:00Z"},
            "value": 18.4,
            "coordinates": {"latitude": 35.1353, "longitude": -106.584702},
            "sensorsId": 3920,
            "locationsId": 2178,
        }

        event = normalize_openaq_latest(location, sensors_by_id, latest, self.ingested_at)

        self.assertEqual(event["source"], "openaq_v3")
        self.assertEqual(event["data_mode"], "measured")
        self.assertEqual(event["station_id"], "2178")
        self.assertEqual(event["country"], "US")
        self.assertEqual(event["city"], "Albuquerque")
        self.assertEqual(event["pollutant"], "pm25")
        self.assertEqual(event["metric_type"], "concentration")
        self.assertEqual(event["value"], 18.4)
        self.assertEqual(event["unit"], "µg/m³")
        self.assertEqual(event["observed_at"], "2026-10-17T08:00:00+00:00")
        self.assertEqual(event["attributions"][0]["name"], "OpenAQ")
        self.assertEqual(event["attributions"][1]["license"], "US Public Domain")
        self.validator.validate(event)

    def test_rejects_openaq_record_without_sensor_metadata(self):
        with self.assertRaises(ValueError):
            normalize_openaq_latest(
                {"id": 2178},
                {},
                {"sensorsId": 3920, "datetime": {"utc": "2026-10-17T08:00:00Z"}, "value": 18.4},
                self.ingested_at,
            )

    def test_collects_latest_records_using_location_sensor_metadata(self):
        responses = {
            "/locations/2178": {
                "results": [
                    {
                        "id": 2178,
                        "locality": "Albuquerque",
                        "country": {"code": "US", "name": "United States"},
                        "licenses": [],
                        "sensors": [
                            {"id": 3920, "parameter": {"name": "pm25", "units": "ug/m3"}}
                        ],
                    }
                ]
            },
            "/locations/2178/latest": {
                "results": [
                    {
                        "datetime": {"utc": "2026-10-17T08:00:00Z"},
                        "value": 18.4,
                        "sensorsId": 3920,
                    }
                ]
            },
        }

        def fake_fetch(path, api_key):
            self.assertEqual(api_key, "test-key")
            return responses[path]

        events = collect_location_events("2178", "test-key", fake_fetch, self.ingested_at)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["pollutant"], "pm25")
        self.assertEqual(events[0]["unit"], "ug/m3")

    def test_synthetic_alert_event_is_explicit_and_schema_valid(self):
        event = build_synthetic_alert_event()

        self.assertEqual(event["source"], "synthetic_alert_test")
        self.assertEqual(event["data_mode"], "synthetic_test")
        self.assertEqual(event["metric_type"], "aqi")
        self.assertGreater(event["value"], 150)
        self.validator.validate(event)

        event = normalize_csv_aqi_row(self.row, self.ingested_at)
        del event["event_id"]
        with self.assertRaises(ValidationError):
            self.validator.validate(event)

    def test_event_id_is_stable_across_replays(self):
        first = normalize_csv_aqi_row(self.row, self.ingested_at)
        second = normalize_csv_aqi_row(
            self.row,
            datetime(2026, 10, 18, 8, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(first["event_id"], second["event_id"])

    def test_rejects_missing_or_invalid_aqi(self):
        for value in (None, "", "not-a-number", "-1", "nan", "inf"):
            with self.subTest(value=value):
                row = dict(self.row)
                row["PM2.5 AQI Value"] = value
                with self.assertRaises(ValueError):
                    normalize_csv_aqi_row(row, self.ingested_at)

    def test_rejects_naive_ingestion_timestamp(self):
        with self.assertRaises(ValueError):
            normalize_csv_aqi_row(self.row, datetime(2026, 10, 17, 8, 30))


if __name__ == "__main__":
    unittest.main()