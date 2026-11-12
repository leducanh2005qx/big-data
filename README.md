# Global Air Quality Monitoring

Dockerized local setup for the Kafka, Spark, InfluxDB, and Grafana air-quality pipeline.

## Requirements

- Docker Desktop with Docker Compose.
- Python 3.12+ for offline tests and optional host-side collectors.
- A Java runtime is only needed when running Spark outside Docker.

## Start the local stack

Hướng dẫn Docker đầy đủ nằm tại `docs/DOCKER_RUN_GUIDE.md`. Cách chạy nhanh nhất sau khi cài Docker Desktop là:

```powershell
.\docker.ps1 start
```

In PowerShell from the project root:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
if (-not (Test-Path .venv)) { py -3.12 -m venv .venv }
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\start.ps1
```

`start.ps1` validates Docker, starts the persistent stack, creates Kafka topics,
provisions InfluxDB retention policies, and starts Spark. It is safe to run again;
it does not remove named volumes. The equivalent manual command is
`docker compose up -d --build`.

The example passwords are for local development only. Replace them in `.env` before sharing or exposing services. Do not commit `.env`.

Wait until ZooKeeper, Kafka, and InfluxDB report healthy; the one-shot `kafka-init` service creates both observation and collection-status topics. Then create the retention policies before Spark starts:

```powershell
.\.venv\Scripts\python.exe -m scripts.influxdb.setup_retention
```

Raw observations are kept for 7 days and hourly aggregates for 30 days by default; existing `autogen` data also receives a 7-day retention period. Spark defaults to a 1 GB driver heap and two local cores; tune `SPARK_DRIVER_MEMORY` and `SPARK_LOCAL_CORES` in `.env` to match the host. MySQL is not used by the current pipeline; start it only when needed:

```powershell
docker compose --profile metadata up -d mysql
```

## Run the demo stream

Open two PowerShell terminals in the project root. Start the Spark processor in one:

```powershell
.\.venv\Scripts\python.exe spark_jobs/stream_processor.py
```

The Spark processor is included in Compose. To use a host-side processor instead,
stop the Compose Spark service and run it in one terminal, then start the CSV replay
producer in another:

```powershell
.\.venv\Scripts\python.exe -m producer.sensor_mock
```

All Python entrypoints load `.env`. The demo producer replays a static CSV snapshot; `ingested_at` is the replay time, not the time the air-quality observation was measured. The event is labeled `historical_csv_replay`; `metric_type=aqi`, `pollutant=pm25`, and `unit=AQI index` make clear that `value` is not PM2.5 concentration. The event contract is defined in `schemas/air_quality_observation.schema.json`.

The Compose stack also starts an `open-meteo-collector` service in continuous mode with `restart: unless-stopped`. Therefore `.\start.ps1` or `docker compose up -d --build` starts the collector together with Kafka, Spark, InfluxDB, and Grafana. Use `.\collect.ps1` only for a host-side one-shot/manual run.

## Run the OpenAQ collector

Register for an OpenAQ API key, then set `OPENAQ_API_KEY` and one or more comma-separated `OPENAQ_LOCATION_IDS` in `.env`. The collector fetches location sensor metadata and that location's latest measurements, then publishes normalized concentration events to Kafka:

```powershell
.\.venv\Scripts\python.exe -m producer.openaq_collector
```

The collector is bounded to 20 locations per run, retries transient rate-limit/server/network failures, isolates a failed location from the rest of the run, and emits a per-location status event to `air_quality_collection_status`. Raw location/latest responses are archived under the gitignored `data/raw/openaq/` directory by default; set `OPENAQ_RAW_ARCHIVE=false` to disable. OpenAQ requires attribution and rate-limits API keys; check the [API key documentation](https://docs.openaq.org/using-the-api/api-key), [rate limits](https://docs.openaq.org/using-the-api/rate-limits), and [terms of use](https://docs.openaq.org/about/terms). Its “latest” endpoint does not guarantee complete capture of measurements ingested out of timestamp order, and OpenAQ does not cover every monitoring station worldwide.

## Dashboard and alert test

Grafana is available at `http://localhost:3000`; use the credentials in `.env`. The dashboard and a demo AQI alert rule are provisioned from files. Replace the placeholder `ALERT_WEBHOOK_URL` in `.env` with a reachable webhook before expecting notification delivery.

With Kafka and Spark running, publish a clearly labeled test event. For a local integration test, start the safe mock receiver first:

```powershell
.\.venv\Scripts\python.exe -m producer.send_synthetic_alert_test
```

Run `\.\venv\Scripts\python.exe scripts/mock_webhook.py --host 0.0.0.0` in another terminal and set `ALERT_WEBHOOK_URL=http://host.docker.internal:8099/grafana-alerts` in `.env`. The mock receiver only proves local delivery; it is not a production notification endpoint.

The event uses `data_mode=synthetic_test`; it is not an air-quality observation. The alert threshold is a pipeline demonstration only. Verify the applicable AQI standard before treating any threshold as a real health warning.

The OpenAQ adapter and offline normalization tests are implemented, but live collection requires your API key and location IDs. Docker Engine is available and the existing ZooKeeper, Kafka, InfluxDB, and Grafana containers have been observed healthy; the new Spark image still needs a completed build before the full Compose pipeline can be verified. Retention expiry and real webhook delivery remain pending.

## Run the second source (Open-Meteo)

Open-Meteo supplies model-estimate concentrations without an API key. Configure
`OPENMETEO_LOCATIONS` as `id|country|city|latitude|longitude`, separated by `;`,
then run:

```powershell
.\.venv\Scripts\python.exe -m producer.open_meteo_collector
```

These records are labeled `data_mode=model_estimate` and must not be presented as
station measurements. The source requires attribution to Open-Meteo and CAMS.

## Tests and CI

Run offline tests with `python -m pytest -q`. GitHub Actions runs compilation,
offline tests, schema/provisioning validation, Compose validation, and a Docker
image build on pushes and pull requests. Live API, webhook, Docker-runtime,
benchmark, and branch-protection checks require the project environment or GitHub
repository settings and are intentionally not faked by CI.

Spark writes raw observations to `raw_7d`, hourly aggregates to `aggregates_30d`, poll status to `air_quality_collection_status`, and malformed Kafka payloads to gitignored `data/quarantine/*.jsonl`. The dashboard separates observation time from collector poll-completion time. Hourly aggregates use only records with `observed_at` and are finalized after the 2-hour event-time watermark, so the CSV replay does not produce hourly observation aggregates.

Open-Meteo hiện được cấu hình cho 34 điểm đại diện cấp tỉnh/thành của Việt Nam theo danh mục hành chính áp dụng từ 01/07/2025. Đây là các điểm trung tâm hành chính/tỉnh lỵ, không phải toàn bộ 3.321 xã/phường, để giữ polling và demo Docker ổn định.

## AI summary V2

The `air-quality-ai` container exposes Vietnamese analysis endpoints at `http://localhost:8090`. It computes statistics from real InfluxDB data, caches the result, writes it to `air_quality_ai_summary`, and Grafana reads the cached summaries through the existing InfluxDB datasource. Configure `AI_API_KEY`, `AI_API_URL`, `AI_MODEL`, `AI_AUTOPUBLISH`, and `AI_SUMMARY_REFRESH_SECONDS` in `.env`; the key is never placed in the dashboard JSON or browser response.

Available summaries are `/summary/overview`, `/summary/aqi-trend`, `/summary/particulate`, and `/summary/compare`. See `docs/AI_DASHBOARD_V2.md` for the response contract and demo flow.

Hướng dẫn đầy đủ cho dự báo, phát hiện bất thường, tư vấn thành phố và email nằm tại [`docs/EXTENDED_FEATURES.md`](docs/EXTENDED_FEATURES.md).

### Phân tích mở rộng

Service AI cũng cung cấp các chức năng:

- `/forecast?city=hanoi&hours=168&horizon=24`: dự báo AQI 24 giờ bằng xu hướng dữ liệu theo giờ; kết quả là ước tính.
- `/anomalies?hours=168`: phát hiện AQI/PM2.5/PM10 bất thường bằng median-MAD.
- `/recommendations?hours=24&limit=5`: xếp hạng thành phố theo AQI 60%, PM2.5 25% và PM10 15%.
- `/email/status`: kiểm tra cấu hình cảnh báo email, không trả về mật khẩu.
- `/email/preview`: xem trước nội dung email cảnh báo, không gửi email.

Trong trang chat có các nút **Dự báo AQI**, **Phát hiện bất thường** và **Tư vấn chọn thành phố**. Các kết quả chỉ dùng dữ liệu thật trong InfluxDB và nêu rõ khi là ước tính.

#### Cấu hình cảnh báo email

Email được gửi từ `SMTP_USERNAME`/`ALERT_EMAIL_FROM` tới danh sách `ALERT_EMAIL_TO`. Có thể gửi từ Gmail của bạn tới chính bạn hoặc tới người khác. Với Gmail, tạo App Password riêng rồi đặt vào `SMTP_PASSWORD`; không dùng mật khẩu đăng nhập Gmail thường.

Để kiểm thử trước, giữ `ALERT_EMAIL_DRY_RUN=true`. Muốn gửi thật, cấu hình SMTP đầy đủ rồi đặt:

```dotenv
ALERT_EMAIL_ENABLED=true
ALERT_EMAIL_DRY_RUN=false
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-sender@gmail.com
SMTP_PASSWORD=your-gmail-app-password
ALERT_EMAIL_FROM=your-sender@gmail.com
ALERT_EMAIL_TO=your-email@example.com,another-recipient@example.com
```

Các ngưỡng `ALERT_AQI_THRESHOLD`, `ALERT_PM25_THRESHOLD` và `ALERT_PM10_THRESHOLD` là ngưỡng vận hành do người dùng cấu hình, không tự khẳng định là ngưỡng y tế.

## Stop services

```powershell
docker compose down
```

Named volumes preserve service data when containers stop. Do not remove volumes unless you intend to delete the local Kafka, InfluxDB, Grafana, and optional MySQL state.
