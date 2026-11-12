# Evaluation Plan

## Test setup record

Fill this in for each final run:

| Item                                 | Value                                |
| ------------------------------------ | ------------------------------------ |
| Run date/time and timezone           | Pending                              |
| Host OS and CPU                      | Pending                              |
| Host RAM                             | Pending                              |
| Docker Desktop memory/CPU allocation | Pending                              |
| Python and PySpark versions          | Python 3.14 / PySpark 4.2.0 (pinned) |
| OpenAQ location IDs                  | Pending                              |
| OpenAQ API request interval          | Pending                              |
| Test duration                        | Pending                              |

## Scenarios

1. Run all unit tests and validate the JSON Schema and Grafana provisioning references.
2. Start Docker Compose and record the health state of ZooKeeper, Kafka, InfluxDB, and Grafana.
3. Apply retention policies and verify `raw_7d`, `aggregates_30d`, and the shortened legacy `autogen` policy.
4. Send one synthetic fixture through Kafka and Spark; verify a raw InfluxDB point and the AQI demo flag.
5. Send measured OpenAQ events for a small, configured set of locations; record source time, ingestion time, pollutant, unit, and attribution.
6. Verify one collection-status event per configured location, including success/partial/failure, `poll_completed_at`, and published/rejected counts.
7. Send a malformed Kafka event and verify the raw payload appears in the ignored JSONL quarantine directory.
8. Wait for an event-time hour to close past the configured watermark and verify the hourly aggregate.
9. Send a synthetic AQI event above 150 and verify Grafana rule evaluation. Verify webhook delivery only with a reachable configured endpoint.
10. Record `docker stats`; separately measure the host Spark JVM because it is not inside a Compose container.

## Metrics

- Throughput: accepted Kafka events divided by measured run duration.
- Freshness: `ingested_at - observed_at` for records with real observation time.
- Collector freshness: current UTC time minus the latest per-location `poll_completed_at`.
- Validation rate: rejected or flagged records divided by all records received.
- Coverage: distinct station IDs and country codes with observations in the run.
- Aggregate correctness: independently calculate expected hourly mean from the test fixture and compare with InfluxDB.
- Resource use: maximum CPU and memory for each container and the host Spark process.
- Retention: policy metadata plus an isolated short-duration test policy in a disposable test database.
- Alert result: expected firing/resolved state and webhook response status.
- Quarantine: invalid payload count and idempotent file behavior for a repeated batch ID.

Keep real observations, historical CSV replay, and synthetic test events clearly separated in all results and screenshots.
