import os
from pathlib import Path
from typing import Iterable


def persist_jsonl_quarantine(
    raw_records: Iterable[str],
    batch_id: int,
    quarantine_dir: Path,
) -> int:
    records = [record for record in raw_records if record]
    if not records:
        return 0

    quarantine_dir.mkdir(parents=True, exist_ok=True)
    destination = quarantine_dir / f"invalid_batch_{batch_id}.jsonl"
    temporary = destination.with_suffix(".jsonl.tmp")
    temporary.write_text("\n".join(records) + "\n", encoding="utf-8")
    os.replace(temporary, destination)
    return len(records)