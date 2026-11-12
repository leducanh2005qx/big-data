from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from spark_jobs.quarantine import persist_jsonl_quarantine


class PersistJsonlQuarantineTests(unittest.TestCase):
    def test_writes_invalid_raw_events_by_batch(self):
        with TemporaryDirectory() as directory:
            count = persist_jsonl_quarantine(
                ['{"invalid":true}', "", "not-json"],
                12,
                Path(directory),
            )

            output = Path(directory) / "invalid_batch_12.jsonl"
            self.assertEqual(count, 2)
            self.assertEqual(output.read_text(encoding="utf-8"), '{"invalid":true}\nnot-json\n')

    def test_reprocessing_the_same_batch_is_idempotent(self):
        with TemporaryDirectory() as directory:
            persist_jsonl_quarantine(["first"], 3, Path(directory))
            persist_jsonl_quarantine(["retry"], 3, Path(directory))

            output = Path(directory) / "invalid_batch_3.jsonl"
            self.assertEqual(output.read_text(encoding="utf-8"), "retry\n")
            self.assertEqual(len(list(Path(directory).iterdir())), 1)

    def test_empty_batch_does_not_create_a_file(self):
        with TemporaryDirectory() as directory:
            count = persist_jsonl_quarantine(["", ""], 4, Path(directory))

            self.assertEqual(count, 0)
            self.assertEqual(list(Path(directory).iterdir()), [])


if __name__ == "__main__":
    unittest.main()