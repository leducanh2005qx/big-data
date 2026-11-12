# Project Plan: Global Air Quality Monitoring

## 1. Project overview

### Purpose

Build a data pipeline that collects air-quality observations from public datasets or APIs, validates and standardizes them, processes them with Spark, stores queryable time-series data, and presents trends, comparisons, and operator alerts in Grafana.

The project is a monitoring and reporting system, not a source of official public-health alerts. Data coverage and freshness depend on each provider and must be shown to dashboard users.

### Current baseline

The repository currently contains:

- `data/global_air_real.csv`: city-level AQI data for several pollutants. The visible columns include AQI values and categories, but no observation timestamp, coordinates, or measured pollutant concentration.
- `producer/sensor_mock.py`: repeatedly reads the CSV and publishes records to Kafka topic `air_quality_data`.
- `producer/openaq_collector.py`: fetches latest observations for explicitly configured OpenAQ locations and publishes them to the same Kafka topic.
- `spark_jobs/stream_processor.py`: reads the Kafka topic, parses the canonical event, and writes metric-typed points to InfluxDB. The demo AQI flag is evaluated only when `metric_type` is `aqi`.
- `docker-compose.yml`: starts Kafka, ZooKeeper, InfluxDB, MySQL, and Grafana.

Important baseline limitations to address before claiming live monitoring:

- The producer maps `PM2.5 AQI Value` to `pollutant=pm25`, `metric_type=aqi`, `value`, and `unit=AQI index`; it marks records as `historical_csv_replay`, leaves `observed_at` null, and records replay time separately as `ingested_at`.
- `schemas/air_quality_observation.schema.json` and `schemas/collection_status.schema.json` define the observation and poll-status contracts; unit tests cover CSV/OpenAQ mapping and invalid values.
- The first real-source adapter targets OpenAQ v3. It archives source payloads, retries transient failures, isolates invalid records, and emits collection status; live collection still requires an API key and configured location IDs.
- The OpenAQ collector now retries transient 429/5xx/network failures with a capped wait, isolates failed locations, counts invalid readings, and archives source JSON under ignored `data/raw/openaq/` by default.
- The collector publishes per-location status to `air_quality_collection_status`, including poll start/end times and published/rejected counts; Spark persists this separately from measurement time for source-freshness reporting.
- The source CSV still has no observation time, coordinates, or measured PM2.5 concentration; replay timestamps must not be interpreted as observation timestamps.
- The synthetic temperature field has been removed from the demo event.
- Kafka and InfluxDB use single-node/demo settings. MySQL is unused by the current pipeline and is now available only through the optional `metadata` Compose profile.
- Credentials are loaded from the local `.env` file using `python-dotenv`; `.env.example` documents required values and `.gitignore` excludes the real `.env`.
- Compose now defines persistent volumes, health checks, and memory/CPU caps for services. Docker runtime was verified with all required services healthy, including the Spark readiness healthcheck.
- `scripts/influxdb/setup_retention.py` provisions 7-day raw and 30-day aggregate policies, and Spark writes raw and hourly data to the matching policies with separate checkpoints.
- Spark routes unparsable or structurally incomplete Kafka events to local JSONL quarantine files and excludes them from InfluxDB writes.
- Hourly aggregates use source `observed_at` and a 2-hour watermark; undated CSV replay records are intentionally excluded.
- OpenAQ emits a separate per-location status topic; Spark stores poll completion time, result counts, and failure state in `air_quality_collection_status` for freshness monitoring.
- Grafana now has provisioned InfluxDB datasource, dashboard, webhook contact point, and a demo AQI alert rule. Runtime datasource queries and alert evaluation were observed; delivery still fails with the placeholder webhook until a real endpoint is configured.
- Spark runs in the Compose container by default, with `SPARK_DRIVER_MEMORY`, `SPARK_LOCAL_CORES`, and a 2 GB container cap; a real Docker stats snapshot was captured, while controlled peak/throughput benchmarking remains pending.

### Implementation status

| Phase                    | Status                                                                                                                                                               |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0. Source selection      | OpenAQ v3 selected; API key and location IDs still need to be configured.                                                                                            |
| 1. Demo baseline         | AQI labels, replay timestamps, `.env`, Compose volumes, health checks, and resource caps are implemented and runtime-verified.                                      |
| 2. Data contract         | Versioned observation and collection-status schemas, CSV/OpenAQ normalizers, attribution, and fixture tests are implemented.                                          |
| 3. Real source           | OpenAQ adapter, capped retries, raw archive, invalid-record counters, and poll-status events are fixture-tested; live API-to-Kafka verification needs a valid key and locations. |
| 4. Processing/storage    | Raw/hourly/status Spark paths, separate checkpoints, quarantine, retention setup, and a live Kafka-to-Spark-to-InfluxDB test are implemented; policy expiry remains pending. |
| 5. Dashboard/alerts      | Trend, AQI, station coverage, latest-observation, and collection-freshness panels plus rule/contact point are provisioned; runtime datasource/alert evaluation passed, webhook endpoint remains pending. |
| 6. Reliability/resources | Compose limits, volumes, health checks, startup script, and local instructions are runtime-verified; controlled benchmark measurements remain pending.                     |
| 7. Second source         | Open-Meteo adapter is live-verified with 288 records and a successful poll-status event; cross-source comparison remains pending until OpenAQ live access is configured. |
| 8. Academic package      | Architecture/data-flow diagrams, report draft, slide outline, demo script, evaluation plan, Docker handoff, CI workflow, and runtime evidence are present; recording and final benchmark remain pending. |

### Current acceptance evidence (2026-10-24)

This table is the authoritative status for the current workspace. Historical runtime notes are not treated as current evidence.

| ID | Acceptance item | Status | Implementation / test | Evidence or blocker |
| --- | --- | --- | --- | --- |
| A01 | Open-Meteo real API adapter for Hanoi, HCMC and Da Nang | PASS (API/parser) | `producer/open_meteo_collector.py`, `.env`; live API response checked for all 3 cities | 2 current AQI + 156 non-future hourly records per city; Kafka delivery not tested while Docker is unavailable |
| A02 | Open-Meteo retry, timeout and HTTP error handling | PASS | `fetch_json`; offline tests | 20-second timeout, capped retries and 429/5xx handling covered by code/tests |
| A03 | Open-Meteo once/continuous collection and deduplication | PASS (code) | `--once`, `--continuous`, persisted source-record IDs | Runtime producer test pending Kafka availability |
| A04 | OpenAQ v3 authenticated collector | BLOCKED | `producer/openaq_collector.py`, fixture tests | Requires user-supplied `OPENAQ_API_KEY` and numeric location IDs |
| A05 | Kafka topics and producer serialization | PASS (configuration) | `docker-compose.yml`, producer modules | Topics are created by `kafka-init`; broker runtime not currently reachable |
| A06 | Spark parsing, quarantine, event-time aggregation and checkpoints | PASS (code) | `spark_jobs/stream_processor.py` | Runtime Spark health and writes require Docker Engine |
| A07 | InfluxDB raw/aggregate/status retention paths | PASS (code/config) | `scripts/influxdb`, Spark writer | Retention expiry behavior is NOT TESTED |
| A08 | Grafana datasource and dashboard panels | PASS (provisioning) | `grafana/`; provisioning tests | Browser/data query verification is NOT TESTED in current run |
| A09 | AQI, PM2.5/PM10 and collector alerts | PARTIAL | alert rules, synthetic producer, mock receiver | Grafana evaluation and webhook delivery are NOT TESTED |
| A10 | Windows operator scripts | PASS (code) | `start.ps1`, `stop.ps1`, `collect.ps1`, `status.ps1`, `test-pipeline.ps1` | Runtime script execution depends on Docker availability |
| A11 | Offline automated tests | PASS | `tests/` | `43 passed` |
| A12 | End-to-end Kafka → Spark → InfluxDB | NOT TESTED | Compose and Spark implementation | Docker Engine returns `Access is denied` |
| A13 | InfluxDB → Grafana live query | NOT TESTED | InfluxDB datasource/dashboard provisioning | InfluxDB/Grafana containers unavailable in current environment |
| A14 | Reproducible benchmark | PENDING | `docs/evaluation-plan.md` | No measured throughput/latency run recorded |
| A15 | CI and branch protection | PARTIAL | `.github/workflows/ci.yml` | Workflow exists; remote run and repository settings not verified |

## 2. Objectives and success criteria

### MVP objectives

1. Ingest the existing CSV as historical/reference data without presenting it as newly observed data.
2. Integrate at least one real external air-quality source after checking its coverage, API terms, limits, and update frequency.
3. Normalize source records into a documented schema with units, source attribution, location, and observation time.
4. Process the stream with Kafka and Spark, persist observations, and visualize them in Grafana.
5. Apply separate retention periods to high-resolution and aggregated time-series data.
6. Notify operators when a correctly identified AQI or concentration metric crosses its configured threshold.
7. Make data freshness, missing values, and source limitations visible to operators and dashboard users.
8. Provide reproducible setup, tests, resource guidance, and academic defense deliverables.

### MVP acceptance criteria

- At least one real source can be polled repeatedly and produces valid normalized records.
- A record retains its source observation time; ingestion time is stored separately.
- AQI and pollutant concentration are represented as different values and are never mislabeled or compared as if they were the same unit.
- Invalid, incomplete, duplicate, and unsupported-unit records are counted or quarantined rather than silently treated as valid measurements.
- A record can be followed from source payload through Kafka and Spark into InfluxDB and a Grafana panel.
- Restarting the processing job does not silently corrupt data or create uncontrolled duplicate observations.
- The dashboard shows the selected time range, pollutant/unit, source, and last successful update.
- Per-location poll freshness is distinct from measurement observation time and includes published/rejected counts.
- A synthetic test event with an explicitly identified metric crosses a configured Grafana alert rule and reaches a test contact point.
- Retention behavior is verified for raw/high-resolution and aggregate measurements.
- A documented local resource profile allows the stack and Spark job to run within the selected host/Docker budget.
- A new developer can follow the README and run the MVP without manually editing source credentials.
- Academic deliverables include architecture/data-flow diagrams, a reproducible evaluation, a final report, and presentation material.

Current execution status: Open-Meteo API and normalization are live-verified for the three configured Vietnamese cities. OpenAQ remains fixture-tested but blocked because no API key/location IDs are configured. Docker-dependent Kafka, Spark, InfluxDB, Grafana, alert delivery and restart checks are not re-verified in the current workspace because Docker Engine returns `Access is denied`; do not label them runtime-verified until `test-pipeline.ps1` completes.

## 3. Scope

### Included in the MVP

- Existing CSV historical/reference ingestion.
- One real air-quality API/provider, selected after source evaluation.
- Optional weather enrichment only if a reliable matching location/time is available; weather is not required for MVP completion.
- Kafka ingestion, Spark validation/normalization and basic windowed aggregations.
- InfluxDB time-series storage with explicit retention policies and Grafana dashboards/alerting.
- MySQL only for station/provider metadata if that metadata is needed; do not add it to the observation path without a concrete use case.
- Local Docker Compose development setup with documented memory/CPU allocation, documentation, and automated tests for transformations and the end-to-end path.
- Academic documentation, diagrams, measured results, and defense materials.

### Not included in the initial MVP

- Guaranteeing worldwide station coverage or true second-by-second real-time data.
- Predictive machine learning, health advice, or official public-health alerts. Grafana notifications are operator alerts for this project, not official health guidance.
- Production high availability, multi-region deployment, authentication for public users, and managed cloud services.
- Combining different AQI standards into one ranking without an explicitly documented conversion method.

## 4. Proposed architecture

```text
Historical CSV / Air-quality provider API / optional weather API
                           |
                 Source-specific collectors
                           |
           Preserve source payload and provenance
                           |
        Validate, map units, normalize timestamps
                           |
             Kafka: air_quality_data
                           |
              Spark Structured Streaming
                    /              \
         InfluxDB observations   Parquet/raw archive (optional)
                    |
           Grafana dashboards
                    |
          Alert contact points
       (Telegram/Discord/Webhook)

MySQL: optional provider, station, and location metadata
```

### Component responsibilities

- **Collectors:** call or read one source each, respect its schedule and rate limits, attach source metadata, retry transient failures, and report collection status.
- **Raw archive:** retain source payloads before destructive transformations. For a local MVP this can be partitioned files under a documented `data/raw/` directory; use Parquet/object storage if volume or replay requirements justify it.
- **Kafka:** decouple collection from processing. Use the existing topic initially; add a dead-letter topic only when invalid-record handling is implemented.
- **Spark:** parse and validate records, normalize units and timestamps, remove duplicates using a stable source identity, and calculate hourly/daily aggregates.
- **InfluxDB:** store normalized time-series observations and aggregates for Grafana queries. Use distinct retention policies for raw/high-resolution and aggregate measurements; archive data elsewhere if longer retention is required.
- **MySQL:** store relatively stable metadata (provider and station registry), not high-volume measurements.
- **Grafana:** present observations, aggregates, coverage, and source freshness; evaluate operator alert rules and send notifications through configured contact points. Do not imply data exists for areas with no stations.
- **Alert contact point:** deliver testable notifications to Telegram, Discord, or a generic webhook. Keep tokens and webhook credentials outside version control.

## 5. Data contract

All collectors should map data to a versioned canonical event. Fields that a source does not provide must remain null or be marked unavailable; do not fabricate coordinates, temperatures, timestamps, or pollutant values.

### Canonical observation fields

| Field                    | Type         | Meaning                                                                    |
| ------------------------ | ------------ | -------------------------------------------------------------------------- |
| `schema_version`         | string       | Contract version, initially `1.0`                                          |
| `event_id`               | string       | Stable deduplication key where source identifiers allow it                 |
| `source`                 | string       | Provider or dataset name                                                   |
| `source_record_id`       | string/null  | Source's observation or record identifier                                  |
| `station_id`             | string/null  | Monitoring station identifier                                              |
| `country`                | string/null  | Country name or standardized country code                                  |
| `city`                   | string/null  | City/locality when provided or reliably resolved                           |
| `latitude` / `longitude` | number/null  | Coordinates in decimal degrees                                             |
| `observed_at`            | ISO-8601 UTC | Time the provider says the observation was made                            |
| `ingested_at`            | ISO-8601 UTC | Time this project received the record                                      |
| `pollutant`              | enum/string  | For example `pm25`, `pm10`, `no2`, `o3`, `co`, `so2`                       |
| `metric_type`            | enum         | `aqi` or `concentration`; keeps unlike measurements separate               |
| `value`                  | number       | Value of the metric identified by `metric_type` and `pollutant`            |
| `unit`                   | string       | `AQI index` for AQI, or explicit concentration unit such as `ug/m3`        |
| `category`               | string/null  | Provider/category label when supplied                                      |
| `aqi_standard`           | string/null  | Standard used for the AQI, when known                                      |
| `quality_flags`          | array/string | Missing, suspect, converted, or provider-specific quality notes            |
| `attributions`           | array        | OpenAQ and upstream provider/license attribution required by the source    |
| `data_mode`              | enum         | `measured`, `model_estimate`, `historical_csv_replay`, or `synthetic_test` |

### Data rules

- Use `metric_type` to distinguish AQI from pollutant concentration. Do not call an AQI value a PM2.5 concentration.
- Preserve the original provider unit and document every conversion. Never compare values across incompatible units.
- Keep `observed_at` and `ingested_at` separate. Historical records without an observation timestamp must be labeled as undated/reference data and excluded from time-series claims that require a measurement time.
- Store timestamps in UTC. Preserve the provider's original timestamp in the raw archive if timezone interpretation is uncertain.
- Use stable provider/station identifiers for deduplication. If no stable ID exists, document the composite key and its limitations.
- Treat country/city names as labels, not unique location identifiers. Prefer station ID and coordinates for joins.
- Never infer a sensor reading from a default value. Synthetic/demo records must be clearly tagged as simulated.
- Alert rules must identify whether the value is AQI or pollutant concentration, include the unit/standard in the rule and notification, and use a threshold appropriate to that metric. A threshold of 150 is not interchangeable between AQI and PM2.5 concentration.

## 6. Delivery phases and work breakdown

The estimates below assume one developer familiar with Python and Docker. Adjust duration to the chosen provider's API and the time available for the project.

### Phase 0: Define the questions and source requirements (2-3 days)

Tasks:

- Select 3-5 questions the dashboard must answer, such as city trend, pollutant comparison, or source freshness.
- Decide whether the main measurement is pollutant concentration, AQI, or both as separate metrics.
- Define the target refresh interval and acceptable staleness; base this on the selected provider, not on a desired label of “real-time”.
- Evaluate candidate providers for coverage, available pollutants, timestamps, coordinates, units, API limits, licensing/attribution, and reliability.
- Use OpenAQ v3 as the first measured-data source. Configure explicit location IDs rather than implying worldwide coverage; preserve the provider and underlying-source attribution.
- OpenAQ requires an API key, documents a free limit of 60 requests/minute and 2,000/hour, and warns that the `latest` resource may not include out-of-order ingested measurements. Keep the collector bounded and expose its configured locations/freshness.
- Review [OpenAQ API key guidance](https://docs.openaq.org/using-the-api/api-key), [rate limits](https://docs.openaq.org/using-the-api/rate-limits), [latest resource](https://docs.openaq.org/resources/latest), and [terms of use](https://docs.openaq.org/about/terms).

Deliverables:

- Short requirements and source-evaluation table.
- A selected first provider and documented constraints.

Exit criteria:

- The team can describe which records the MVP can and cannot represent and what freshness to expect.

### Phase 1: Stabilize the existing demo path (3-5 days)

Tasks:

- Rename the existing CSV fields accurately; preserve AQI values as AQI rather than PM2.5 concentration.
- Remove or clearly tag synthetic temperature and generated timestamps.
- Make the CSV producer behavior explicitly a historical/demo replay, not a live sensor.
- Confirm the JSON schema used by producer, Spark, and InfluxDB agrees.
- Put credentials in environment variables or a local environment file excluded from version control.
- Add Compose volumes and health checks where supported; verify start-up order and service readiness.

Deliverables:

- A truthful demo stream with documented limitations.
- A local configuration example without real secrets.

Exit criteria:

- A sample CSV record has the same meaning at producer, Spark, InfluxDB, and Grafana.

### Phase 2: Define the data contract and quality checks (3-5 days)

Tasks:

- Implement the canonical observation schema and schema version.
- Keep the machine-readable schema in `schemas/air_quality_observation.schema.json` and make each source adapter emit that contract.
- Define accepted pollutants, units, timestamp parsing, null behavior, and quality flags.
- Define deduplication identity and invalid-record behavior.
- Add unit tests for source mapping, unit handling, invalid values, stable event identity, and malformed records. Initial CSV mapping tests are in place; extend them as real sources are added.

Deliverables:

- Schema documentation and sample valid/invalid events.
- Transformation and validation tests.

Exit criteria:

- Tests prove AQI is not mapped as concentration and missing measurement time is not replaced with current time.

### Phase 3: Build the first real-source collector (1-2 weeks)

Tasks:

- Implement one provider adapter, configured through environment variables/settings.
- The initial OpenAQ adapter calls `/v3/locations/{id}` and `/v3/locations/{id}/latest`, joining `sensorsId` to location sensor metadata to determine pollutant and unit.
- Respect provider pagination, rate limits, attribution requirements, and update interval.
- Record `observed_at`, `ingested_at`, source ID, station/location, pollutant, value, and unit when provided.
- Retry transient failures with bounded backoff; log permanent errors without losing the whole collection run.
- Save location and latest raw responses under `data/raw/openaq/` for debugging and reproducibility, subject to provider terms; disable with `OPENAQ_RAW_ARCHIVE=false` if needed.
- Publish canonical events to Kafka and report the number collected, rejected, and published.
- Publish a separate status event for every configured location, including failed polls, so freshness does not depend on observation timestamps.

Deliverables:

- One collector that can run once or on a configured schedule.
- Example raw and canonical records with secrets removed.

Exit criteria:

- A live collection run publishes valid records to Kafka, and provider failures are visible in logs/status.
- Live validation is only considered complete after a valid user API key and at least one configured location have been tested; fixture tests do not count as live-source verification.
- Fixture tests verify transient retry/backoff, raw response archive, invalid-record isolation, and poll-status event structure.

### Phase 4: Process and persist observations (1 week)

Tasks:

- Update Spark schema and parser to the canonical event; route malformed records to a logged/quarantined path.
- Validate ranges and units conservatively. Flag suspicious values; do not silently discard without counters.
- Deduplicate using stable event identity where possible.
- Add hourly aggregates only after validating observation timestamps and time-window behavior.
- Configure checkpointing and test restart/replay behavior for the chosen Spark/Kafka setup.
- Write observations and aggregates to InfluxDB with tags/fields designed for Grafana queries.
- Consume collection-status events and persist poll completion time, status, and record counts to the dedicated status measurement.
- Persist invalid raw Kafka payloads to gitignored JSONL quarantine files and exclude them from InfluxDB writes.
- Create an InfluxDB 1.8 retention-policy script for high-resolution observations (initial target: 7 days) and aggregates (initial target: 30 days); make durations configurable and verify which policy each write/query uses.
- Preserve longer-term history only in the raw/Parquet archive when required; do not treat the InfluxDB retention policy as the archival copy.
- Add persistence and service configuration for local development.

Deliverables:

- Spark job that processes the real-source topic and writes queryable measurements.
- Collection-status measurement, JSONL quarantine, documented InfluxDB layout, and retention-policy setup/verification script.

Exit criteria:

- A test event can be traced into InfluxDB once, and a job restart behaves as documented.

### Phase 5: Build Grafana dashboards and alerting (4-6 days)

Panels:

- Time-series trend for a selected pollutant, station/city, and time range.
- City/station comparison for the same pollutant, unit, and time range.
- Top 15 cities/stations only when records are comparable by pollutant, unit, period, and AQI standard.
- Global/selected-region summary panel with its aggregation method and coverage limitations stated.
- AQI category/value panel only when AQI source/standard is known.
- Coverage panel showing stations/cities with observations in the selected period.
- Source freshness panel showing last successful poll and latest observation time.
- Per-location collection freshness panel showing poll completion time and published/rejected counts.
- Station coverage panel is a sample-count view, not a global sensor inventory or a measure of all available monitoring stations.

Tasks:

- Configure the InfluxDB data source and dashboard variables.
- Make units and AQI standard visible in titles/legends.
- Show “no data” distinctly from “good air quality”.
- Configure a Grafana alert rule and a Telegram, Discord, or generic webhook contact point using secrets stored outside the repository.
- Test alert delivery with a clearly tagged synthetic event. Include metric name, value, unit/AQI standard, location, and source in the notification.
- Treat `150` as a configurable example threshold, not a universal PM2.5 concentration threshold. Identify whether the rule evaluates AQI or concentration and select the applicable standard/threshold.
- Export dashboard provisioning/configuration so it can be recreated.

Deliverables:

- Version-controlled MVP dashboard and a screenshot/example query set.
- Version-controlled alert-rule/contact-point provisioning without credentials, plus evidence of a test notification.

Exit criteria:

- A user can identify metric, unit, location, time period, source, and freshness without reading implementation code.
- A synthetic test event fires the expected rule and sends a notification; normal/no-data cases do not produce false “safe” messages.

### Phase 6: Reliability, security, resource management, and documentation (4-6 days)

Tasks:

- Externalize credentials and document local secret configuration.
- Add bounded retries, timeouts, structured logs, and basic counters for collection and processing.
- Add Docker health checks, persistent volumes, and restart policies suitable for local development.
- Record host RAM/CPU and Docker Desktop allocation; define a reproducible resource budget for each service and for the Spark process.
- Configure compatible Docker Compose memory/CPU limits for containers and Spark driver/executor JVM settings. Validate the effective Compose configuration and observe use with `docker stats` during the end-to-end run.
- Keep the unused MySQL service out of the default run profile or document its measured resource cost until metadata storage is needed.
- Document data retention and cleanup for local volumes/raw files.
- Run an end-to-end test with one sample event and one provider collection.
- Document limitations, troubleshooting, shutdown, and reset procedures.

Deliverables:

- README setup/run guide, `.env.example`, test instructions, and known-limitations section.
- End-to-end verification results.
- A measured local resource profile with peak memory/CPU; container limits must not be mistaken for limits on Spark when it runs on the host.

Exit criteria:

- A clean checkout can be started using the documented steps, and failures can be diagnosed from logs/status.

### Phase 7: Add a second source and compare coverage (after MVP)

Tasks:

- Add a second provider only after the first source uses the canonical contract successfully.
- Compare overlap by pollutant, unit, station, time, and location.
- Document differences in station coverage, measurement frequency, and AQI calculation.
- Add source selection and provenance to Grafana.

Deliverables:

- Second adapter and source-comparison report.

Exit criteria:

- Records from both sources can be distinguished and are only compared when their meanings and units are compatible.

### Phase 8: Academic evaluation and defense preparation (1 week, final)

Tasks:

- Draw system architecture and data-flow diagrams showing source, ingestion, Kafka, Spark, storage, Grafana, and alert delivery.
- Write the final report with problem statement, objectives, source evaluation, design, implementation, testing, results, limitations, and references/data attribution.
- Measure and report throughput, end-to-end freshness/latency, invalid/duplicate record counts, data coverage, and peak CPU/RAM for a documented test run.
- Prepare presentation slides covering the problem, pipeline, design choices, results, and limitations.
- Record a short demo showing ingestion, dashboard updates, and an alert fired by a tagged synthetic test event.
- Re-run the demo from documented setup and keep screenshots/log evidence with secrets removed.

Deliverables:

- Architecture diagram and data-flow diagram in `docs/`.
- Final report, presentation slides, test/evaluation results, and demo recording or demo script.

Exit criteria:

- Results are reproducible from documented steps, sources are attributed, and simulated alert data is clearly distinguished from real observations.

## 7. Proposed repository layout

```text
data/
  historical/                 # Existing reference CSV and documented provenance
  raw/                        # Local raw responses; exclude secrets and large generated files as appropriate
producer/
  collectors/                 # One adapter per real provider
  sensor_mock.py              # Clearly labeled demo-only replay source
schemas/
  air_quality_observation.schema.json
  collection_status.schema.json
spark_jobs/
  stream_processor.py
  tests/
grafana/
  provisioning/
    datasources/
    dashboards/
    alerting/
  dashboards/
scripts/
  influxdb/                    # Retention policy setup and verification
data/
  quarantine/                  # Invalid Kafka payloads; local and gitignored
tests/
  unit/
  integration/
docker-compose.yml
.env.example
README.md
PROJECT_PLAN.md
docs/                          # Diagrams, report, slides, evaluation evidence
```

Create only the directories needed as each phase is implemented. Do not commit API keys, passwords, or raw provider data if the provider terms prohibit redistribution.

## 8. Testing and verification plan

### Unit tests

- Map each source payload to the canonical schema.
- Parse UTC timestamps and reject or flag ambiguous/missing timestamps.
- Verify AQI and concentration fields remain distinct.
- Verify unit conversion is explicit and tested.
- Verify malformed or incomplete records are rejected/quarantined with a reason.
- Verify deduplication key stability.
- Verify transient OpenAQ errors retry with a capped delay and permanent client errors do not retry.
- Verify raw source response archiving and malformed-event quarantine in isolated temporary directories.
- Verify per-location collection status is written independently from observation timestamps.

### Integration tests

- Collector publishes a known fixture to Kafka.
- Spark parses the fixture and writes the expected InfluxDB measurement.
- Duplicate fixture behavior is deterministic.
- Invalid fixture is visible in logs/counters or quarantine output.
- Spark restart/checkpoint behavior is verified for the chosen configuration.

### Manual dashboard check

- Select a pollutant, unit, location, and time range.
- Confirm no-data is not presented as a zero reading or “good” reading.
- Confirm source and latest observation/ingestion time are visible.
- Confirm poll completion time and poll record counts are shown separately from observation time.
- Confirm multiple AQI standards or incompatible units are not combined into an unlabeled comparison.

### Retention and alert checks

- Verify InfluxDB reports the configured raw/high-resolution and aggregate retention policies and that new points use the intended policy.
- Use a short-lived test policy or controlled test database to verify expiration; do not wait seven or thirty days or delete project data just to test retention.
- Inject a synthetic event with a documented metric and unit (for example, a test AQI value above its configured AQI threshold) and verify Grafana alert evaluation and contact-point delivery.
- Verify notifications do not disclose API keys, tokens, or webhook URLs.

## 9. Risks and mitigations

| Risk                                                                 | Mitigation                                                                                                                                           |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Provider has limited geographic coverage or delayed updates          | Display coverage and freshness; choose a second source only after MVP; avoid claiming complete global coverage.                                      |
| Different sources use different units, station IDs, or AQI standards | Keep source provenance, define conversions explicitly, and compare only compatible observations.                                                     |
| Historical data is mistaken for current data                         | Keep observation time separate from ingestion time; label undated data and exclude it from current-time panels.                                      |
| API limits or outages interrupt collection                           | Use bounded retries, schedule according to provider limits, and expose last-successful-poll status.                                                  |
| Duplicate records after retry/restart                                | Use stable source IDs and idempotent writes where possible; test Spark checkpoint/replay semantics.                                                  |
| Secrets or credentials are committed                                 | Use environment variables/local secret files excluded from version control; rotate any exposed credentials.                                          |
| Large data volume exceeds local setup                                | Start with one source and limited retention; add Parquet/object storage or distributed deployment only when measured volume requires it.             |
| AQI calculation differs by region/provider                           | Prefer provider AQI with its standard recorded; if calculating AQI, implement and document a named standard rather than one global implicit formula. |
| Retention removes data needed for analysis                           | Keep high-resolution and aggregate policies separate, test policy selection, and archive records outside InfluxDB when longer history is required.   |
| Threshold gives misleading alert                                     | Label AQI versus concentration and its standard/unit; configure a threshold for the selected metric rather than treating `150` as universal.         |
| Docker limits fail to constrain Spark                                | Spark currently runs on the host; configure its JVM separately or containerize it, then measure host and container resource use.                     |
| Alert credentials leak or alerts spam                                | Keep contact-point secrets outside source control, use a test channel, and test evaluation/recovery and notification behavior.                       |
| Academic claims exceed evidence                                      | Record source coverage, freshness, test conditions, and resource measurements; distinguish replay/test data from external observations.              |

## 10. Suggested schedule

| Week  | Focus                                                                       | Milestone                                     |
| ----- | --------------------------------------------------------------------------- | --------------------------------------------- |
| 1     | Requirements, source evaluation, baseline corrections                       | Data contract and first provider selected     |
| 2     | Schema, quality rules, tests, collector skeleton                            | Canonical sample events pass tests            |
| 3     | First real-source collector and Kafka publishing                            | Real records visible in Kafka                 |
| 4     | Spark processing, deduplication, InfluxDB                                   | Real records queryable in InfluxDB            |
| 5     | Grafana dashboard and freshness/coverage panels                             | Dashboard MVP usable                          |
| 6     | Grafana alerting, retention verification, resource tuning, end-to-end tests | Demo-ready MVP with alert and bounded storage |
| 7     | Evaluation, report, diagrams, slides, demo rehearsal                        | Project defense package ready                 |
| Later | Second source, historical backfill, scale improvements                      | Multi-source comparison                       |

The schedule is a planning estimate, not a dependency on a particular API. If provider access or field quality is poor, use that time to evaluate another source rather than inventing missing measurements.

## 11. Demo flow

1. Start the local services with Docker Compose and verify Kafka, InfluxDB, and Grafana are healthy.
2. Run the selected real-source collector once; show collection status and a sample canonical event with secrets removed.
3. Confirm the event reaches Kafka, is processed by Spark, and appears in InfluxDB.
4. Open Grafana and inspect the time-series panel, comparable city ranking, coverage, and source freshness.
5. Inject a tagged synthetic test event and show the alert notification arriving at the test contact point.
6. Show the documented limitations: source coverage, update frequency, AQI standard, and any unavailable fields.
7. Briefly show retention-policy configuration and the measured local resource profile.

## 12. Immediate next actions

1. Start Docker Desktop and confirm the Linux engine is available.
2. Create `.env` only if it does not already exist; set an OpenAQ API key, location IDs, and a reachable webhook URL.
3. Run `docker compose up -d`, verify health checks, and run `python -m scripts.influxdb.setup_retention`.
4. Start Spark, run the OpenAQ collector, and verify observation plus collection-status records through Kafka, InfluxDB, and Grafana.
5. Run `python -m producer.send_synthetic_alert_test`, verify webhook delivery, and record resource/evaluation metrics for the report.

## 13. V2 upgrade addendum (02/11/2026)

The existing pipeline was upgraded without changing Kafka topics, observation measurements, retention policies, or named volumes. The AI service now computes real-data summaries for overview, AQI trend, particulate matter, and city comparison; caches them by data fingerprint; and persists them to `air_quality_ai_summary` for Grafana. The dashboard has 17 Vietnamese panels and one geomap with AQI, PM2.5, and PM10 layers. Offline verification after the upgrade is `43 passed`; Docker runtime verification after the upgrade is pending because Docker Engine was unavailable during the audit. See `docs/V2_UPGRADE_AUDIT.md` and `docs/AI_DASHBOARD_V2.md`.
