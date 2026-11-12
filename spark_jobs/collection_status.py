from datetime import datetime, timezone
from typing import Mapping, Any


def to_influx_collection_status_point(event: Mapping[str, Any]) -> dict[str, Any]:
    required = (
        "status_id",
        "source",
        "location_id",
        "status",
        "poll_started_at",
        "poll_completed_at",
        "records_published",
        "records_rejected",
    )
    missing = [field for field in required if event.get(field) is None]
    if missing:
        raise ValueError(f"Collection status is missing required fields: {', '.join(missing)}")

    if event["status"] not in {"success", "partial", "failed"}:
        raise ValueError("Collection status must be success, partial, or failed")

    completed_at = event["poll_completed_at"]
    if isinstance(completed_at, str):
        completed_at = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    if not isinstance(completed_at, datetime) or completed_at.utcoffset() is None:
        raise ValueError("poll_completed_at must be a timezone-aware timestamp")
    completed_at = completed_at.astimezone(timezone.utc)

    tags = {
        "source": str(event["source"]),
        "location_id": str(event["location_id"]),
        "status": str(event["status"]),
    }
    fields: dict[str, Any] = {
        "status_id": str(event["status_id"]),
        "poll_started_at": str(event["poll_started_at"]),
        "poll_completed_at": completed_at.isoformat(),
        "records_published": int(event["records_published"]),
        "records_rejected": int(event["records_rejected"]),
    }
    if event.get("error"):
        fields["error"] = str(event["error"])

    return {
        "measurement": "air_quality_collection_status",
        "tags": tags,
        "time": completed_at.isoformat(),
        "fields": fields,
    }