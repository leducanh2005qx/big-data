import os
from pathlib import Path

from dotenv import load_dotenv
from influxdb import InfluxDBClient

from .retention import ensure_retention_policies


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    username = os.getenv("INFLUXDB_ADMIN_USER")
    password = os.getenv("INFLUXDB_ADMIN_PASSWORD")
    if not username or not password:
        raise RuntimeError("Set INFLUXDB_ADMIN_USER and INFLUXDB_ADMIN_PASSWORD in .env")

    client = InfluxDBClient(
        host=os.getenv("INFLUXDB_HOST", "127.0.0.1"),
        port=int(os.getenv("INFLUXDB_PORT", "8086")),
        username=username,
        password=password,
        database=os.getenv("INFLUXDB_DATABASE", "air_quality"),
    )
    try:
        statements = ensure_retention_policies(
            client,
            os.getenv("INFLUXDB_DATABASE", "air_quality"),
            os.getenv("INFLUXDB_RAW_RETENTION", "7d"),
            os.getenv("INFLUXDB_AGGREGATE_RETENTION", "30d"),
        )
        for statement in statements:
            print(f"Applied: {statement}")
    finally:
        client.close()


if __name__ == "__main__":
    main()