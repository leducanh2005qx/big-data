# Final handoff and verification status

This document records implementation status without treating documentation or
fixtures as runtime evidence.

## Implementation status

| Area | Status | Evidence or blocker |
|---|---|---|
| Phases 1–6 baseline | IMPLEMENTED | Existing collectors, schemas, Spark paths, Grafana provisioning, retention, quarantine, and resource settings |
| Phase 7 second source | LIVE VERIFIED | Open-Meteo live poll for Hanoi published 288 records; Spark stored them and the status event reported `success`, `records_rejected=0`. |
| Docker packaging | PASS | Apache Spark 4.2.0 image built; Compose Spark, Kafka, InfluxDB, Grafana, and ZooKeeper are running; `start.ps1` completed successfully and Spark healthcheck is healthy. |
| CI validation | IMPLEMENTED | `.github/workflows/ci.yml`; GitHub-hosted run has not been observed from this workspace |
| OpenAQ live collection | BLOCKED | Requires a valid API key and configured location IDs |
| End-to-end pipeline | PASS FOR TEST/LIVE SOURCES | Container Spark processed the synthetic event and live Open-Meteo records from Kafka into InfluxDB; restart recovery returned Spark to healthy state with persistent volumes. |
| Webhook delivery | FAIL/EXPECTED CONFIGURATION BLOCKER | Grafana runtime attempted delivery and DNS failed for the placeholder `example.invalid`; a reachable test webhook is still required. |
| Retention expiry | NOT RUN | Policies were provisioned and used; isolated expiry behavior still needs a short-lived policy test |
| Benchmarks | PARTIAL | A real Docker stats snapshot was captured; controlled throughput/latency/peak measurements are still pending |
| Auto-merge / branch protection | NOT RUN | Requires GitHub repository administration settings |

## Offline verification

The test suite covers the existing CSV/OpenAQ behavior, quarantine, retention
configuration, Grafana references, and the new Open-Meteo mapping. The authoritative
command is:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

If `pytest` is missing, install `requirements.txt` first. A test result is only
recorded after that command actually completes.

## Runtime acceptance checklist

- [x] `docker compose config` succeeds.
- [x] `.\start.ps1` starts healthy services without removing volumes.
- [ ] OpenAQ publishes a live event with a valid key and location.
- [x] Open-Meteo publishes model-estimate events; 288 records and status are queryable.
- [x] Kafka → host Spark → InfluxDB path is observed with logs and a query for a tagged synthetic event.
- [x] Grafana datasource/alert evaluation is observed in runtime logs; dashboard visual inspection remains manual.
- [ ] Retention policies and isolated expiry test are observed.
- [ ] Synthetic alert reaches the configured test webhook.
- [x] CPU/RAM snapshot and restart recovery are measured; controlled throughput/latency benchmark remains pending.
- [ ] GitHub Actions passes on the repository and branch protection blocks failed merges.

## V2 upgrade status (02/11/2026)

- AI service now provides structured `/summary/overview`, `/summary/aqi-trend`, `/summary/particulate`, and `/summary/compare` endpoints.
- AI summaries are computed from real InfluxDB statistics, cached by data fingerprint, and persisted as `air_quality_ai_summary` for Grafana queries.
- Grafana dashboard has 17 panels, four AI summary panels beside the charts/map, and one interactive geomap with AQI, PM2.5 and PM10 layers.
- Offline test result after V2 changes: `43 passed, 1 pytest cache warning`.
- Docker runtime after V2 changes is not currently verified because Docker Engine was unavailable during this audit.
- OpenAQ live collection, real webhook delivery, retention-expiry test, and controlled benchmark remain pending.
