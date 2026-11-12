import json
import unittest
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from producer.openaq_collector import build_collection_status_event
from spark_jobs.collection_status import to_influx_collection_status_point


class CollectionStatusPointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "collection_status.schema.json"
        with schema_path.open(encoding="utf-8") as schema_file:
            cls.validator = Draft202012Validator(
                json.load(schema_file),
                format_checker=FormatChecker(),
            )

    def setUp(self):
        self.event = {
            "event_type": "collection_status",
            "status_id": "status-1",
            "source": "openaq_v3",
            "location_id": "2178",
            "status": "partial",
            "poll_started_at": "2026-10-19T12:00:00+00:00",
            "poll_completed_at": "2026-10-19T12:00:04+00:00",
            "records_published": 7,
            "records_rejected": 2,
            "error": None,
        }

    def test_maps_status_to_a_separate_influx_measurement(self):
        point = to_influx_collection_status_point(self.event)

        self.assertEqual(point["measurement"], "air_quality_collection_status")
        self.assertEqual(point["tags"]["location_id"], "2178")
        self.assertEqual(point["tags"]["status"], "partial")
        self.assertEqual(point["fields"]["records_published"], 7)
        self.assertEqual(point["fields"]["records_rejected"], 2)
        self.assertEqual(point["time"], "2026-10-19T12:00:04+00:00")

    def test_rejects_missing_or_invalid_status_fields(self):
        missing = dict(self.event)
        del missing["location_id"]
        with self.assertRaises(ValueError):
            to_influx_collection_status_point(missing)

        invalid = dict(self.event, status="unknown")
        with self.assertRaises(ValueError):
            to_influx_collection_status_point(invalid)

    def test_rejects_naive_completion_time(self):
        invalid = dict(self.event, poll_completed_at=datetime(2026, 10, 19, 12, 0))
        with self.assertRaises(ValueError):
            to_influx_collection_status_point(invalid)

    def test_collector_status_event_matches_versioned_schema(self):
        started = datetime(2026, 10, 19, 12, 0, tzinfo=datetime.now().astimezone().tzinfo)
        completed = datetime(2026, 10, 19, 12, 0, 4, tzinfo=datetime.now().astimezone().tzinfo)
        event = build_collection_status_event("2178", "success", started, completed, 3)

        self.validator.validate(event)


if __name__ == "__main__":
    unittest.main()