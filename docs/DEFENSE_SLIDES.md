# Defense Presentation Outline

## Slide 1: Title and motivation

- Global Air Quality Monitoring
- Why multi-source air-quality data is difficult to compare
- Project scope: a reproducible Big Data ingestion and reporting prototype

## Slide 2: Problem and objectives

- Sources differ in unit, timestamp, location coverage, and AQI standard
- Preserve data provenance and distinguish measurements from AQI
- Stream, process, store, visualize, and demonstrate operator alerting

## Slide 3: Data sources

- Historical CSV: AQI values/categories; no timestamp or coordinates
- OpenAQ v3: selected station measurements with units and timestamps
- Coverage is limited to configured locations, not the whole planet
- Cite OpenAQ and the underlying provider/license attribution

## Slide 4: Architecture

- Show `docs/architecture.md` system diagram
- CSV/OpenAQ adapters -> Kafka -> Spark -> InfluxDB -> Grafana
- Raw 7-day and aggregate 30-day retention policies

## Slide 5: Canonical schema and data quality

- Explain `metric_type`, `value`, `unit`, `observed_at`, `ingested_at`
- Explain the separate collection-status contract and why poll time differs from measurement time
- Show why AQI is not PM2.5 concentration
- Explain stable event IDs, missing coordinates, and attribution

## Slide 6: Streaming and processing

- Explain Kafka decoupling and Spark Structured Streaming
- Raw event path, invalid-event quarantine, and one-hour aggregation with a 2-hour watermark
- Separate checkpoints and retention policies
- Separate observation freshness from per-location collection freshness

## Slide 7: Grafana dashboard

- Hourly PM2.5 concentration trend
- Highest hourly readings panel and latest observation table
- Station sample coverage and last collector poll times, distinct from observation timestamps
- Country filter and AQI panel labeled as demo until the standard is known

## Slide 8: Alert demonstration

- State clearly that the example AQI threshold is not an official health warning
- Run the synthetic test producer
- Show the rule state and webhook receiver only when an endpoint is configured

## Slide 9: Evaluation

- Present measured test results from `docs/evaluation-plan.md`
- Include throughput, freshness, coverage, invalid records, and resource use
- Do not present pending values as measured results

## Slide 10: Limitations and next steps

- OpenAQ requires a key; latest data may omit out-of-order arrivals
- Docker runtime and live API measurements need to be captured on the project machine
- Add source two, verify source licenses/coverage, and finalize real alert thresholds
