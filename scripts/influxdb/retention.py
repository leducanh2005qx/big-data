import re
from typing import Any


RETENTION_POLICIES = (
    ("raw_7d", "INFLUXDB_RAW_RETENTION"),
    ("aggregates_30d", "INFLUXDB_AGGREGATE_RETENTION"),
)


def _validate_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Invalid InfluxDB {label}: {value!r}")
    return value


def _validate_duration(value: str, label: str) -> str:
    if not re.fullmatch(r"[1-9][0-9]*[smhdw]", value):
        raise ValueError(f"Invalid {label} duration: {value!r}")
    return value


def ensure_retention_policies(
    client: Any,
    database: str,
    raw_duration: str = "7d",
    aggregate_duration: str = "30d",
) -> list[str]:
    database = _validate_identifier(database, "database name")
    raw_duration = _validate_duration(raw_duration, "raw retention")
    aggregate_duration = _validate_duration(aggregate_duration, "aggregate retention")

    existing_policies = {
        str(row["name"])
        for row in client.query(f'SHOW RETENTION POLICIES ON "{database}"').get_points()
    }
    statements = []

    for name, duration in (
        ("raw_7d", raw_duration),
        ("aggregates_30d", aggregate_duration),
    ):
        operation = "ALTER" if name in existing_policies else "CREATE"
        default_clause = " DEFAULT" if name == "raw_7d" else ""
        statement = (
            f'{operation} RETENTION POLICY "{name}" ON "{database}" '
            f"DURATION {duration} REPLICATION 1{default_clause}"
        )
        client.query(statement)
        statements.append(statement)

    if "autogen" in existing_policies:
        statement = (
            f'ALTER RETENTION POLICY "autogen" ON "{database}" '
            f"DURATION {raw_duration} REPLICATION 1"
        )
        client.query(statement)
        statements.append(statement)

    return statements