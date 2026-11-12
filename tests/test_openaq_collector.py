import json
from datetime import datetime, timezone
from email.message import Message
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError

from producer.openaq_collector import build_collection_status_event, collect_location_events, fetch_json


class OpenAqCollectorResilienceTests(unittest.TestCase):
    def test_retries_transient_http_failure_with_bounded_backoff(self):
        headers = Message()
        response = HTTPError("https://api.openaq.org/v3/locations/2178", 503, "unavailable", headers, None)
        opener = Mock(side_effect=[response, BytesIO(b'{"results": []}')])
        sleep = Mock()

        result = fetch_json("/locations/2178", "test-key", opener=opener, sleep=sleep)

        self.assertEqual(result, {"results": []})
        self.assertEqual(opener.call_count, 2)
        sleep.assert_called_once_with(1.0)

    def test_uses_retry_after_header_but_caps_the_wait(self):
        headers = Message()
        headers["Retry-After"] = "90"
        response = HTTPError("https://api.openaq.org/v3/locations/2178", 429, "limited", headers, None)
        opener = Mock(side_effect=[response, BytesIO(b'{"results": []}')])
        sleep = Mock()

        fetch_json("/locations/2178", "test-key", opener=opener, sleep=sleep)

        sleep.assert_called_once_with(5)

    def test_does_not_retry_permanent_http_errors(self):
        headers = Message()
        response = HTTPError("https://api.openaq.org/v3/locations/2178", 401, "unauthorized", headers, None)
        opener = Mock(side_effect=response)

        with self.assertRaisesRegex(RuntimeError, "HTTP 401"):
            fetch_json("/locations/2178", "test-key", opener=opener, sleep=Mock())

        opener.assert_called_once()

    def test_retries_socket_timeout(self):
        opener = Mock(side_effect=[TimeoutError("request timed out"), BytesIO(b'{"results": []}')])
        sleep = Mock()

        result = fetch_json("/locations/2178", "test-key", opener=opener, sleep=sleep)

        self.assertEqual(result, {"results": []})
        self.assertEqual(opener.call_count, 2)
        sleep.assert_called_once_with(1.0)

    def test_archives_raw_location_and_latest_payloads(self):
        location = {
            "id": 2178,
            "locality": "Albuquerque",
            "country": {"code": "US", "name": "United States"},
            "sensors": [{"id": 3920, "parameter": {"name": "pm25", "units": "ug/m3"}}],
        }
        latest = {
            "results": [{
                "datetime": {"utc": "2026-10-17T08:00:00Z"},
                "value": 18.4,
                "sensorsId": 3920,
            }]
        }
        responses = {"/locations/2178": {"results": [location]}, "/locations/2178/latest": latest}
        collection_time = datetime(2026, 10, 19, 12, 30, tzinfo=timezone.utc)

        def fake_fetch(path, api_key):
            if api_key != "test-key":
                raise AssertionError("unexpected fixture API key")
            return responses[path]

        with TemporaryDirectory() as directory:
            events = collect_location_events(
                "2178",
                "test-key",
                fetch=fake_fetch,
                ingested_at=collection_time,
                raw_archive_dir=Path(directory),
            )
            archive_files = sorted(Path(directory).glob("*.json"))

            self.assertEqual(len(events), 1)
            self.assertEqual(len(archive_files), 2)
            archived_payloads = [json.loads(path.read_text(encoding="utf-8")) for path in archive_files]
            self.assertTrue(any(payload == latest for payload in archived_payloads))

    def test_invalid_measurement_is_skipped_without_losing_valid_records(self):
        location = {
            "id": 2178,
            "sensors": [{"id": 3920, "parameter": {"name": "pm25", "units": "ug/m3"}}],
        }
        latest = {
            "results": [
                {"datetime": {"utc": "2026-10-17T08:00:00Z"}, "value": -1, "sensorsId": 3920},
                {"datetime": {"utc": "2026-10-17T09:00:00Z"}, "value": 18.4, "sensorsId": 3920},
            ]
        }
        responses = {"/locations/2178": {"results": [location]}, "/locations/2178/latest": latest}

        def fake_fetch(path, api_key):
            if api_key != "test-key":
                raise AssertionError("unexpected fixture API key")
            return responses[path]

        events = collect_location_events("2178", "test-key", fake_fetch)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["value"], 18.4)

    def test_rejects_non_numeric_location_id_before_api_calls(self):
        fetch = Mock()

        with self.assertRaisesRegex(ValueError, "numeric"):
            collect_location_events("../../unexpected", "test-key", fetch)

        fetch.assert_not_called()

    def test_builds_collection_status_with_explicit_poll_times_and_counts(self):
        started = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
        completed = datetime(2026, 10, 19, 12, 0, 4, tzinfo=timezone.utc)

        event = build_collection_status_event(
            location_id="2178",
            status="partial",
            poll_started_at=started,
            poll_completed_at=completed,
            records_published=7,
            records_rejected=2,
        )

        self.assertEqual(event["event_type"], "collection_status")
        self.assertEqual(event["status"], "partial")
        self.assertEqual(event["poll_started_at"], started.isoformat())
        self.assertEqual(event["poll_completed_at"], completed.isoformat())
        self.assertEqual(event["records_published"], 7)
        self.assertEqual(event["records_rejected"], 2)


if __name__ == "__main__":
    unittest.main()