# Hướng dẫn đóng gói và chạy Docker

Tài liệu này là cách chạy chuẩn cho AirQuality Big Data V2 trên Windows bằng Docker Desktop và PowerShell.

## 1. Thành phần trong gói

Không cần cài Kafka, Spark, InfluxDB hoặc Grafana riêng trên máy. Compose tự chạy các service:

| Service | Vai trò | Cổng |
|---|---|---:|
| ZooKeeper | Quản lý Kafka | 2181 |
| Kafka | Message broker | 9092 |
| Kafka init | Tạo topic tự động | nội bộ |
| Spark | Streaming, quarantine, aggregate | nội bộ |
| Open-Meteo collector | Thu thập dữ liệu liên tục | nội bộ |
| InfluxDB | Lưu observation, aggregate, AI summary | 8086 |
| Grafana | Dashboard và cảnh báo | 3000 |
| Air-quality AI | AI summary và hỏi đáp tiếng Việt | 8090 |

Dữ liệu và checkpoint nằm trong named volumes Docker. Không dùng `down -v` trong demo thông thường.

## 2. Yêu cầu cài đặt một lần

1. Cài Docker Desktop cho Windows.
2. Bật Linux containers và Docker Engine.
3. Cài Python 3.12 trở lên nếu muốn chạy test hoặc script retention trên host.
4. Mở PowerShell tại thư mục project:

```powershell
cd F:\bigdata\AirQuality_BigData
```

## 3. Cấu hình `.env`

Nếu chưa có file `.env`:

```powershell
Copy-Item .env.example .env
```

Tối thiểu kiểm tra các biến:

```env
INFLUXDB_ADMIN_USER=airquality_dev
INFLUXDB_ADMIN_PASSWORD=đặt-mật-khẩu-local
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=đặt-mật-khẩu-grafana
OPENMETEO_LOCATIONS=hanoi|VN|Hanoi|21.0285|105.8542;haiphong|VN|Hai Phong|20.8449|106.6881;halong|VN|Ha Long|20.9599|107.0425;vinh|VN|Vinh|18.6796|105.6813;hue|VN|Hue|16.4637|107.5909;danang|VN|Da Nang|16.0544|108.2022;quynhon|VN|Quy Nhon|13.7829|109.2196;nhatrang|VN|Nha Trang|12.2388|109.1967;dalat|VN|Da Lat|11.9404|108.4583;hcm|VN|Ho Chi Minh City|10.8231|106.6297;bienhoa|VN|Bien Hoa|10.9574|106.8426;cantho|VN|Can Tho|10.0452|105.7469;buonmathuot|VN|Buon Ma Thuot|12.6667|108.0500
AI_API_KEY=key-của-bạn
AI_API_URL=https://api.openai.com/v1/chat/completions
AI_MODEL=gpt-4o-mini
```

Không commit `.env`, không đưa API key vào dashboard JSON và không gửi key qua giao diện web.

## 4. Lệnh chạy nhanh nhất

Chỉ cần một lệnh:

```powershell
.\docker.ps1 start
```

Nếu Windows chặn chạy file `.ps1`, dùng file đóng gói không cần chỉnh Execution Policy:

```powershell
.\run-docker.cmd
```

Hoặc double-click `run-docker.cmd` trong thư mục project.

Lệnh này sẽ:

- kiểm tra Docker Engine;
- tạo `.env` từ `.env.example` nếu còn thiếu;
- validate Compose;
- build image Spark dùng chung cho Spark, collector và AI;
- khởi động toàn bộ service;
- chờ healthcheck;
- cấu hình retention policy InfluxDB;
- in trạng thái service và URL truy cập.

Cách tương đương:

```powershell
.\start.ps1
```

Nếu cần chạy trực tiếp trong môi trường có Execution Policy chặt:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\docker.ps1 start
```

## 5. Truy cập sau khi chạy

- Grafana: `http://localhost:3000`
- AI assistant: `http://localhost:8090`
- AI health: `http://localhost:8090/health`
- InfluxDB ping: `http://localhost:8086/ping`

Đăng nhập Grafana bằng `GRAFANA_ADMIN_USER` và `GRAFANA_ADMIN_PASSWORD` trong `.env`.

Dashboard được provision tự động với tên **HỆ THỐNG GIÁM SÁT CHẤT LƯỢNG KHÔNG KHÍ**.

## 6. Các lệnh vận hành thường dùng

```powershell
# Xem trạng thái
.\docker.ps1 status

# Theo dõi log toàn bộ stack
.\docker.ps1 logs

# Xem riêng pipeline
docker compose logs -f open-meteo-collector spark influxdb

# Dừng service nhưng giữ dữ liệu
.\docker.ps1 stop

# Khởi động lại sau khi đã stop
.\docker.ps1 start

# Build lại sau khi sửa code/provisioning
.\docker.ps1 rebuild
```

## 7. Kiểm thử sau khi chạy

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\test-pipeline.ps1
```

Kiểm tra AI summary:

```powershell
Invoke-RestMethod http://localhost:8090/health | ConvertTo-Json
Invoke-RestMethod "http://localhost:8090/summary/overview?hours=24" | ConvertTo-Json -Depth 8
Invoke-RestMethod "http://localhost:8090/summary/particulate?city=hanoi&hours=24" | ConvertTo-Json -Depth 8
```

## 8. Cập nhật project về sau

Sau khi pull/copy source mới:

```powershell
cd F:\bigdata\AirQuality_BigData
.\docker.ps1 rebuild
```

Lệnh này build lại image và recreate container nhưng không xóa named volumes.

## 9. Dữ liệu có được giữ lại không?

Có. `docker compose stop`, `docker compose down` và `docker.ps1 rebuild` giữ các named volumes:

- Kafka data;
- InfluxDB data;
- Grafana data;
- Spark checkpoint;
- quarantine;
- trạng thái dedup Open-Meteo.

Không chạy lệnh sau nếu không thực sự muốn xóa dữ liệu:

```powershell
docker compose down -v
```

## 10. Lỗi thường gặp

### Docker Engine unavailable

Mở Docker Desktop, chờ biểu tượng Docker báo Running rồi thử:

```powershell
docker info
.\docker.ps1 start
```

### Service chưa healthy

```powershell
docker compose ps
docker compose logs --tail 200 zookeeper kafka influxdb spark grafana air-quality-ai
```

### Grafana không có dữ liệu

Kiểm tra Spark và collector:

```powershell
docker compose logs --tail 200 open-meteo-collector spark
.\docker.ps1 status
```

Chọn `Last 24 hours` hoặc `Last 7 days` tùy dữ liệu hiện có. Không đổi timestamp cũ thành thời gian hiện tại.

### AI fallback thay vì gọi OpenAI

Kiểm tra cấu hình mà không in API key:

```powershell
docker compose exec air-quality-ai python3 -c "import os; print({'key_configured': bool(os.getenv('AI_API_KEY','').strip()), 'model': os.getenv('AI_MODEL')})"
```

Nếu đã sửa `.env`, chạy lại:

```powershell
docker compose up -d --force-recreate air-quality-ai
```
