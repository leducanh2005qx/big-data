import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

from .observation import build_observation_event


def build_synthetic_alert_event(value: float = 151.0) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return build_observation_event(
        source="synthetic_alert_test",
        data_mode="synthetic_test",
        source_record_id=None,
        station_id="synthetic-alert-test",
        country="TEST",
        city="Synthetic Alert Test",
        latitude=None,
        longitude=None,
        observed_at=now,
        ingested_at=now,
        pollutant="pm25",
        metric_type="aqi",
        value=value,
        unit="AQI index",
        category="Synthetic test",
        aqi_standard="test-only",
        quality_flags=["synthetic_alert_test"],
        attributions=[],
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    value = float(os.getenv("AQI_ALERT_TEST_VALUE", "151"))
    event = build_synthetic_alert_event(value)

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )
    try:
        producer.send("air_quality_data", value=event).get(timeout=30)
    finally:
        producer.close()

    print(f"Published synthetic AQI alert test event {event['event_id']} with value {value}")


if __name__ == "__main__":
    main()