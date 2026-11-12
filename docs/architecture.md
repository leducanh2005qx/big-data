# Architecture and Data Flow

## System architecture

```mermaid
flowchart LR
    CSV[Historical CSV replay] --> CSVAdapter[CSV adapter]
    OpenAQ[OpenAQ v3 API] --> OpenAQAdapter[Location and latest adapter]
    CSVAdapter --> Kafka[Kafka topic: air_quality_data]
    OpenAQAdapter --> Kafka
    OpenAQAdapter --> StatusTopic[Kafka topic: air_quality_collection_status]
    Kafka --> Spark[Spark Structured Streaming]
    StatusTopic --> StatusSpark[Spark status consumer]
    Spark --> Quarantine[Ignored JSONL invalid-event quarantine]
    Spark -->|Raw observations, 7-day RP| InfluxRaw[(InfluxDB: raw_7d)]
    Spark -->|Hourly aggregates, 30-day RP| InfluxHourly[(InfluxDB: aggregates_30d)]
    StatusSpark -->|Poll completion and counts| InfluxStatus[(InfluxDB: air_quality_collection_status)]
    InfluxStatus --> Grafana
    InfluxRaw --> Grafana[Grafana dashboards]
    InfluxHourly --> Grafana
    Grafana -->|AQI demo alert| Webhook[Configured webhook contact point]
    MySQL[(Optional station metadata)] -. optional .-> OpenAQAdapter
```

The CSV path is a replay for pipeline demonstrations. It carries an AQI metric without a measured observation timestamp. OpenAQ observations use physical concentration units and source-provided UTC timestamps. The two metric types remain distinct in the canonical event and in InfluxDB tags.

## Event data flow

```mermaid
sequenceDiagram
    participant API as OpenAQ v3
    participant Adapter as OpenAQ collector
    participant Kafka as Kafka observations
    participant Status as Kafka collection status
    participant Spark
    participant Quarantine as JSONL quarantine
    participant Raw as InfluxDB raw_7d
    participant Hourly as InfluxDB aggregates_30d
    participant Poll as InfluxDB collection status
    participant Grafana

    Adapter->>API: GET location metadata
    Adapter->>API: GET location latest values
    API-->>Adapter: Sensor IDs, pollutant/unit metadata, values, UTC times
    Adapter->>Adapter: Normalize, validate, preserve attribution
    Adapter->>Kafka: Publish versioned observation event
    Adapter->>Status: Publish per-location poll result
    Kafka->>Spark: Stream event
    Spark->>Quarantine: Save malformed event payload
    Spark->>Raw: Write observation using observed_at
    Spark->>Spark: Event-time window, 1-hour group, 2-hour watermark
    Spark->>Hourly: Write finalized hourly aggregate
    Status->>Spark: Stream poll completion and counts
    Spark->>Poll: Write collection status
    Grafana->>Raw: Query latest/raw views
    Grafana->>Hourly: Query trend and comparison panels
    Grafana-->>Grafana: Evaluate demo AQI rule
    Grafana->>Grafana: Send to configured webhook contact point
```

## Operational boundaries

- OpenAQ access requires an API key. The collector polls only explicitly configured location IDs and does not represent total global coverage.
- The API's latest resource may not capture measurements ingested out of time order.
- The demo AQI rule is not an official health warning. Select the correct AQI standard before using any threshold operationally.
- Docker resource caps apply to Compose containers. Spark currently runs as a host process and needs its own measured JVM budget.
- Historical raw data retention beyond the InfluxDB policies belongs in a separate archive, not in the 7-day raw retention policy.
- Measurement freshness (`ingested_at - observed_at`) and collector freshness (`poll_completed_at`) are separate metrics; a new poll does not guarantee a new measurement.
