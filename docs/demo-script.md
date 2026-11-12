# Three-Minute Project Demo

## Before recording

- Start Docker Desktop and confirm the Linux engine is available.
- Create `.env` from `.env.example` if needed. Configure an OpenAQ API key, at least one location ID, and a reachable webhook URL.
- Start the stack with `docker compose up -d` and wait for Kafka, InfluxDB, and Grafana health checks.
- Run `python -m scripts.influxdb.setup_retention` before Spark.
- Open Grafana at `http://localhost:3000` and prepare the Global Air Quality dashboard.

## Recording script

### 0:00-0:25 — Problem and scope

“This project demonstrates a streaming data pipeline for air-quality observations. It does not claim complete worldwide coverage or provide official health warnings. AQI and physical pollutant concentration are stored as different metric types.”

Show the architecture diagram in `docs/architecture.md`.

### 0:25-1:10 — Ingestion and processing

- Start Spark in one terminal with `.venv` and show its Kafka subscription.
- In another terminal, run `python -m producer.openaq_collector` for the configured location IDs.
- Show a sanitized event or log with source, station, pollutant, value, unit, `observed_at`, and `ingested_at`.
- Show the separate collection-status event with poll completion time and published/rejected counts.
- Point out the JSONL quarantine path when explaining malformed payload handling.
- Explain the event-time window and that finalized hourly aggregates wait for the 2-hour watermark.

If OpenAQ access is unavailable during the defense, run `python -m producer.sensor_mock` and explicitly label its records as historical CSV replay.

### 1:10-2:00 — Dashboard and retention

- Open the provisioned dashboard and select a country.
- Show the PM2.5 concentration trend and highest hourly readings panel.
- Show the separate AQI panel and explain that its color bands are demo bands until the source AQI standard is confirmed.
- Show that raw observations and hourly aggregates use separate retention policies.
- Show the collection-freshness panel separately from observation timestamps.

### 2:00-2:35 — Alert demonstration

- Run `python -m producer.send_synthetic_alert_test`.
- Show the event labeled `synthetic_test`, the Grafana rule transition, and the webhook notification if the configured endpoint is reachable.
- State that the synthetic event proves alert plumbing only; it is not an environmental reading.

### 2:35-3:00 — Results and limitations

- Present only measured values from `docs/evaluation-plan.md`.
- State the configured OpenAQ station coverage, API polling limits, and Docker/Spark resource measurements.
- Note pending or unavailable live results rather than estimating them.

## Evidence to capture

- Health status for Kafka, InfluxDB, and Grafana.
- One sanitized real-source event and its resulting InfluxDB point.
- Dashboard panels with source, pollutant, unit, time range, and freshness visible.
- The synthetic event, alert state, and webhook response.
- `docker stats` output and separate host Spark process resource use.
