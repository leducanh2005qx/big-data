# Audit nâng cấp AirQuality Big Data V2

Ngày audit: 02/11/2026

## Kết luận nhanh

Pipeline hiện tại vẫn giữ nguyên luồng:

`Open-Meteo → Kafka → Spark Structured Streaming → InfluxDB → Grafana`

Đợt V2 bổ sung lớp phân tích AI có cấu trúc và lưu kết quả cache vào InfluxDB để Grafana truy vấn trực tiếp. Không thay Kafka topic, measurement quan sát, retention policy hoặc named volume.

## Đã có trước khi nâng cấp

- Collector Open-Meteo liên tục, retry, timeout, lọc timestamp tương lai và deduplication.
- Spark ghi observation vào `raw_7d`, hourly aggregate vào `aggregates_30d`, collection status vào `air_quality_collection_status`.
- Grafana datasource UID `air-quality-influx`, biến `country`, `city`, `source`, dashboard dark mode tiếng Việt.
- AI Q&A tại port 8090, API key chỉ truyền vào container qua `.env`.
- Docker khởi động bằng `.\start.ps1`.

## Đã chỉnh trong V2

- `scripts/air_quality_ai.py`
  - Thêm `/health`.
  - Thêm `/summary/overview`.
  - Thêm `/summary/aqi-trend?city=hanoi&hours=24`.
  - Thêm `/summary/particulate?city=hanoi&hours=24`.
  - Thêm `/summary/compare?hours=24`.
  - Tính thống kê bằng Python từ dữ liệu InfluxDB trước khi gọi AI.
  - Cache theo scope, thành phố, khoảng thời gian, fingerprint dữ liệu và phiên bản logic.
  - Ghi summary vào measurement `air_quality_ai_summary` trong retention policy `raw_7d`.
  - Có fallback thống kê thật khi thiếu key hoặc OpenAI lỗi.
  - Có worker cập nhật summary định kỳ, mặc định 600 giây; Grafana refresh không gọi OpenAI trực tiếp.
- `grafana/dashboards/air-quality-overview.json`
  - Tổ chức lại thành 17 panel.
  - Có panel AI cạnh biểu đồ AQI, bụi mịn, so sánh và bản đồ.
  - Bản đồ có lớp AQI, PM2.5 và PM10 cùng một geomap.
  - AQI dùng ngưỡng US-AQI khi dữ liệu ghi `US-AQI`; bụi dùng thang màu liên tục, không áp dụng ngưỡng AQI lên nồng độ.
- `docker-compose.yml`, `.env`, `.env.example`
  - Thêm `AI_AUTOPUBLISH` và `AI_SUMMARY_REFRESH_SECONDS`.
- `tests/test_air_quality_ai.py`, `tests/test_grafana_provisioning.py`
  - Bổ sung test summary, cache, fallback, bảo vệ API key và ba lớp bản đồ.
- `docs/AI_DASHBOARD_V2.md`
  - Mô tả kiến trúc, API, cache, panel và kịch bản demo.

## Rủi ro và giới hạn

- Docker Engine đang tắt ở lượt audit này nên chưa xác minh runtime sau thay đổi V2.
- OpenAQ vẫn cần API key và location IDs; Open-Meteo là nguồn live đã cấu hình.
- Grafana dùng InfluxDB summary cache; các panel AI hiển thị summary đã ghi, không nhúng key hoặc gọi REST từ trình duyệt.
- Benchmark throughput/latency, retention expiry và webhook thật vẫn cần chạy trong môi trường Docker.

## Rollback an toàn

Bản sao an toàn của dashboard V2 hiện tại được lưu tại `docs/air-quality-overview.v2.backup.json`. Không xóa named volumes. Khi cần rollback về bản trước V2, dùng lịch sử version control hoặc bản backup tương ứng; không dùng bản sao này để giả định đó là bản cũ. Khi cần khôi phục, chỉ thay file dashboard sau khi đã dừng provisioning hoặc khôi phục nội dung có kiểm soát, rồi restart Grafana.

## Lệnh kiểm thử

```powershell
.\.venv\Scripts\python.exe -m pytest -q
docker compose config
.\start.ps1
.\status.ps1
.\test-pipeline.ps1
```

