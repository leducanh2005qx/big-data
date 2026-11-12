# Global Air Quality Monitoring

## Abstract

This project builds a prototype data pipeline for receiving, validating, processing, storing, and visualizing air-quality observations. Kafka decouples source adapters from Spark Structured Streaming; InfluxDB stores time-series observations and hourly aggregates; Grafana presents trends and a configurable operator alert. The system distinguishes AQI from pollutant concentration and preserves source, location, unit, observation time, ingestion time, and attribution.

The repository contains a historical CSV replay and an OpenAQ v3 adapter. The CSV provides AQI values but no observation timestamps or coordinates. The OpenAQ adapter retries transient failures, archives raw responses locally, quarantines invalid stream payloads, and emits a separate per-location poll-status stream. It is fixture-tested; live-source results remain pending a user API key and configured location IDs. Runtime measurements must be filled in after Docker services are available.

## 1. Problem statement

Air-quality datasets are distributed across providers and differ in units, timestamps, location metadata, and reporting standards. Combining them without preserving these distinctions can create misleading comparisons. The project demonstrates a repeatable ingestion and processing workflow while making those data-quality constraints visible.

## 2. Objectives

- Ingest the existing CSV as explicitly labeled historical replay data.
- Ingest current measurements from selected OpenAQ locations.
- Normalize source records to a versioned event contract.
- Process the Kafka stream with Spark and persist time-series data in InfluxDB.
- Apply separate retention policies to raw and hourly data.
- Provide Grafana trend, comparison, coverage, freshness, and demo-alert views.
- Evaluate correctness, freshness, throughput, and local resource use.

## 3. Scope and data sources

### Historical CSV

The CSV contains city-level AQI values and categories for multiple pollutants. The current producer uses `PM2.5 AQI Value` as `metric_type=aqi`, `pollutant=pm25`, and `unit=AQI index`. It does not convert that AQI to PM2.5 mass concentration. Because the file has no measurement time or coordinates, the event has a null `observed_at` and a current `ingested_at`, and is tagged `historical_csv_replay`.

### OpenAQ v3

The collector combines location sensor metadata with the location latest endpoint to obtain pollutant, unit, value, coordinates, and UTC measurement time. Location IDs are explicitly configured. OpenAQ requires an API key and attribution to OpenAQ and, where required, the underlying data provider. The latest endpoint does not guarantee capture of all late-ingested measurements.

## 4. System design

The observation and collection-status JSON Schemas are `schemas/air_quality_observation.schema.json` and `schemas/collection_status.schema.json`. The observation contract distinguishes AQI and concentration using `metric_type`, retains `value` and `unit`, and records source attribution. The status event records per-location poll start/end times, outcome, and record counts. Spark writes raw points to `raw_7d`, hourly event-time aggregates to `aggregates_30d` after a 2-hour watermark, and status to a separate freshness measurement. Malformed Kafka payloads are written to local JSONL quarantine. Each stream has its own checkpoint.

Grafana provisions its InfluxDB datasource, dashboard, webhook contact point, and AQI demo alert from files under `grafana/provisioning/`. The webhook endpoint must be configured in the local `.env`. A synthetic alert producer exists for testing and marks its records `data_mode=synthetic_test`.

## 5. Data quality and safety

- AQI is never labeled as pollutant concentration.
- Units are retained as provided; cross-unit comparisons are filtered out.
- `observed_at` is never fabricated. `ingested_at` is separately recorded.
- Poll completion time is recorded separately from observation time; a poll with no new measurement still updates source-health status.
- Missing source coordinates remain null and are flagged.
- Stable event IDs do not depend on replay time.
- The AQI threshold in the demo rule is not a universal health threshold. It must be associated with the applicable AQI standard before operational use.
- OpenAQ coverage does not represent all monitoring stations worldwide.

## 6. Evaluation methodology

Run unit and provisioning tests first, then the local end-to-end procedure in `README.md`. Record results only from an actual run; do not substitute synthetic fixtures for OpenAQ measurements.

| Metric                    | Method                                                                  | Result                                      |
| ------------------------- | ----------------------------------------------------------------------- | ------------------------------------------- |
| Normalization correctness | Schema and adapter unit tests                                           | 34 offline tests pass                       |
| End-to-end freshness      | Compare `observed_at` with `ingested_at` for live events                | Synthetic and Open-Meteo paths runtime-verified |
| Collector freshness       | Current UTC time minus latest `poll_completed_at` per configured location | Open-Meteo Hanoi status: success, 288 published, 0 rejected |
| Throughput                | Valid events written divided by measured test duration                  | Pending runtime                             |
| Invalid/rejected records  | Collector/Spark counters divided by received records                    | Pending runtime counters                    |
| Geographic coverage       | Unique configured countries/stations with observations                  | Pending live API run                        |
| Peak memory/CPU           | Docker stats plus host Spark process measurement                        | Docker snapshot captured; not a peak benchmark |
| Alert delivery            | Synthetic event and configured webhook receiver                         | Rule evaluation observed; webhook blocked by `example.invalid` |
| Retention                 | Query `SHOW RETENTION POLICIES` and test an isolated short-lived policy | Policies provisioned/used; expiry test pending |

## 7. Current verification status

The repository has 34 offline tests for CSV/OpenAQ/Open-Meteo mapping, transient retry/backoff, raw response archiving, invalid-event quarantine, collection-status mapping, retention-policy SQL generation, synthetic alerts, and Grafana provisioning references. Docker Compose built and ran the Spark container; Kafka → Spark → InfluxDB was verified with a tagged synthetic event and a live Open-Meteo poll. OpenAQ live collection still requires a user API key and location IDs. Webhook delivery reached Grafana evaluation but failed against the placeholder endpoint, and controlled peak/throughput/latency benchmarks plus retention expiry remain pending.

## 8. Limitations and future work

- Compare the live Open-Meteo model-estimate stream with OpenAQ only after OpenAQ credentials and a live station run are configured.
- Measure raw-archive disk usage and set a separate cleanup policy if longer replay or auditing is needed.
- Verify source-status behavior during prolonged API outages and Kafka unavailability.
- Measure Spark JVM resources separately from Docker container limits.
- Confirm source-specific AQI standards before ranking or alerting on AQI values.
- Revisit rate limits and API terms before increasing OpenAQ polling frequency or location count.

## References

- OpenAQ API key: https://docs.openaq.org/using-the-api/api-key
- OpenAQ latest resource: https://docs.openaq.org/resources/latest
- OpenAQ rate limits: https://docs.openaq.org/using-the-api/rate-limits
- OpenAQ terms: https://docs.openaq.org/about/terms
- Grafana file provisioning: https://grafana.com/docs/grafana/latest/administration/provisioning/
- InfluxDB v1 retention policies: https://docs.influxdata.com/influxdb/v1/query_language/manage-database/
- Open-Meteo Air Quality API: https://open-meteo.com/en/docs/air-quality-api
