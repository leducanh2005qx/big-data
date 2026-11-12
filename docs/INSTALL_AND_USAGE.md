# Hướng dẫn cài đặt và sử dụng AirQuality Big Data

Tài liệu cho Windows 10/11 và PowerShell. Thư mục dự án là `F:\bigdata\AirQuality_BigData`.

## 1. Yêu cầu

- Docker Desktop, Linux containers và Docker Engine đang chạy.
- Python 3.12 trở lên.
- RAM khuyến nghị tối thiểu 8 GB; Docker nên được cấp khoảng 4 GB RAM trở lên.

Kiểm tra:

```powershell
python --version
docker --version
docker compose version
docker info
```

Nếu `docker info` báo `Access is denied`, hãy mở Docker Desktop và chờ Docker Engine ở trạng thái Running.

## 2. Cài Python và thư viện

```powershell
cd F:\bigdata\AirQuality_BigData
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Nếu PowerShell không cho activate, dùng trực tiếp:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Thư viện chính gồm `kafka-python`, `influxdb`, `python-dotenv`, `pyspark`, `jsonschema`, `PyYAML` và `pytest`. Docker image Spark tự cài các thư viện trong `requirements-docker.txt` khi build.

## 3. Cấu hình `.env`

```powershell
Copy-Item .env.example .env
```

Nếu `.env` đã tồn tại thì không ghi đè. Các biến quan trọng:

```env
INFLUXDB_DATABASE=air_quality
INFLUXDB_ADMIN_USER=airquality_dev
INFLUXDB_ADMIN_PASSWORD=thay-bang-mat-khau-local
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=thay-bang-mat-khau-local
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
OPENMETEO_LOCATIONS=hanoi|VN|Hanoi|21.0285|105.8542;haiphong|VN|Hai Phong|20.8449|106.6881;halong|VN|Ha Long|20.9599|107.0425;vinh|VN|Vinh|18.6796|105.6813;hue|VN|Hue|16.4637|107.5909;danang|VN|Da Nang|16.0544|108.2022;quynhon|VN|Quy Nhon|13.7829|109.2196;nhatrang|VN|Nha Trang|12.2388|109.1967;dalat|VN|Da Lat|11.9404|108.4583;hcm|VN|Ho Chi Minh City|10.8231|106.6297;bienhoa|VN|Bien Hoa|10.9574|106.8426;cantho|VN|Can Tho|10.0452|105.7469;buonmathuot|VN|Buon Ma Thuot|12.6667|108.0500
OPENMETEO_INTERVAL_SECONDS=600
OPENMETEO_DEDUP_FILE=data/state/open_meteo_seen_ids.txt
```

Định dạng địa điểm là `id|country|city|latitude|longitude`; nhiều địa điểm cách nhau bằng dấu `;`. Không commit `.env`, password, API key hoặc webhook token.

## 4. Khởi động hệ thống

Cách khuyến nghị:

```powershell
cd F:\bigdata\AirQuality_BigData
.\start.ps1
```

Script sẽ kiểm tra Docker, validate Compose, build Spark image, khởi động ZooKeeper/Kafka/InfluxDB/Spark/Grafana, tạo Kafka topics và cấu hình retention policy.

Cách thủ công:

```powershell
docker compose up -d --build
docker compose ps
```

`kafka-init` kết thúc với exit code `0` là bình thường vì đây là init job chạy một lần.

## 5. Kiểm thử offline

```powershell
.\.venv\Scripts\python.exe -m pytest -q
docker compose config --quiet
```

Kiểm tra JSON/YAML Grafana:

```powershell
.\.venv\Scripts\python.exe -c "import json; from pathlib import Path; import yaml; json.loads(Path('grafana/dashboards/air-quality-overview.json').read_text(encoding='utf-8')); [yaml.safe_load(p.read_text(encoding='utf-8')) for p in Path('grafana').rglob('*.yaml')]; print('config ok')"
```

## 6. Chạy Open-Meteo

Open-Meteo không cần API key. Khi chạy `start.ps1` hoặc `docker compose up -d --build`, service `open-meteo-collector` trong Docker sẽ tự chạy liên tục theo `OPENMETEO_INTERVAL_SECONDS` và tự khởi động lại nếu bị dừng.

Chạy một lần thủ công để demo hoặc kiểm tra:

```powershell
.\collect.ps1
```

Hoặc:

```powershell
.\.venv\Scripts\python.exe -m producer.open_meteo_collector --once
```

Nếu muốn chạy collector ngoài Docker:

```powershell
.\collect.ps1 -Continuous
```

Hoặc chỉ định chu kỳ tối thiểu 300 giây:

```powershell
.\.venv\Scripts\python.exe -m producer.open_meteo_collector --continuous --interval-seconds 600
```

Collector gửi observation vào topic `air_quality_data` và trạng thái polling vào `air_quality_collection_status`. Dữ liệu Open-Meteo có `data_mode=model_estimate`, không phải số đo sensor.

## 7. Chạy OpenAQ

OpenAQ cần API key thật. Cập nhật `.env`:

```env
OPENAQ_API_KEY=your-real-api-key
OPENAQ_LOCATION_IDS=1234,5678
OPENAQ_RAW_ARCHIVE=true
```

Sau đó chạy:

```powershell
.\.venv\Scripts\python.exe -m producer.openaq_collector
```

Không dùng location ID hoặc API key giả. Nếu thiếu key, chương trình sẽ báo lỗi cấu hình và dừng.

## 8. Kiểm tra Kafka, Spark và InfluxDB

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic air_quality_data --from-beginning --max-messages 5
docker compose logs -f spark
```

Kiểm tra InfluxDB và retention:

```powershell
Invoke-WebRequest http://127.0.0.1:8086/ping
.\.venv\Scripts\python.exe -m scripts.influxdb.setup_retention
```

`raw_7d` lưu observation/status; `aggregates_30d` lưu hourly aggregate. Không dùng `docker compose down -v` trong demo thông thường vì lệnh này xóa volume, dữ liệu và checkpoint.

## 9. Mở Grafana

Truy cập `http://localhost:3000`. Đăng nhập bằng `GRAFANA_ADMIN_USER` và `GRAFANA_ADMIN_PASSWORD` trong `.env`. Dashboard `Global Air Quality` được provision tự động, gồm PM2.5, PM10, CO, NO2, SO2, O3, US AQI, European AQI, city/source filter và collector status.

## 10. Alert và mock webhook

Terminal riêng:

```powershell
.\.venv\Scripts\python.exe scripts/mock_webhook.py --host 0.0.0.0
```

Trong `.env`:

```env
ALERT_WEBHOOK_URL=http://host.docker.internal:8099/grafana-alerts
```

Khởi động lại Grafana rồi gửi synthetic event:

```powershell
.\.venv\Scripts\python.exe -m producer.send_synthetic_alert_test
```

Synthetic event chỉ kiểm tra đường đi alert, không phải dữ liệu môi trường thật.

## 11. Script vận hành

```powershell
.\test-pipeline.ps1   # pytest, Compose validation và smoke test
.\status.ps1          # trạng thái service, InfluxDB, Grafana
.\stop.ps1            # dừng service, giữ volume
docker compose logs --tail 100 kafka spark influxdb grafana
```

## 12. Demo nhanh

Chỉ cần Terminal 1:

```powershell
.\start.ps1
```

Collector, Kafka, Spark, InfluxDB và Grafana sẽ chạy trong Docker. Theo dõi log tùy chọn:

```powershell
docker compose logs -f open-meteo-collector spark
```

Mở Grafana, chọn `Last 24 hours`, trình bày dữ liệu nhiều thành phố Việt Nam, `model_estimate`, timestamp, aggregation và collection status.

## 13. Lỗi thường gặp

- `Docker Engine unavailable / Access is denied`: mở Docker Desktop, kiểm tra Linux containers rồi chạy `docker info`.
- `Kafka connection refused`: chạy `docker compose ps kafka` và `docker compose logs --tail 100 kafka`. Producer Windows dùng `127.0.0.1:9092`; Spark Compose dùng `kafka:29092`.
- Open-Meteo thiếu địa điểm: kiểm tra `Select-String OPENMETEO_LOCATIONS .env`; giá trị phải đúng dạng `id|country|city|latitude|longitude`.
- OpenAQ thiếu API key: đây là blocker cấu hình bình thường; không dùng key giả hoặc thay OpenAQ bằng Open-Meteo.
- Grafana không có dữ liệu: kiểm tra `docker compose ps`, log Spark và chạy lại `.\collect.ps1`; time range phải bao phủ thời điểm collector chạy.
