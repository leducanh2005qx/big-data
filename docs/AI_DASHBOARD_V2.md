# AI và Dashboard Grafana V2

## Kiến trúc

AI service chạy trong container `air-quality-ai` tại `http://localhost:8090`.

1. Service truy vấn observation thật từ InfluxDB `raw_7d`.
2. Python tính thống kê theo scope và khoảng thời gian.
3. Chỉ các thống kê đã kiểm chứng được gửi tới OpenAI-compatible API.
4. Kết quả được cache theo fingerprint dữ liệu.
5. Summary được ghi vào measurement `air_quality_ai_summary`.
6. Grafana truy vấn measurement này bằng datasource InfluxDB hiện có.

API key không xuất hiện trong dashboard JSON, HTML hoặc response `/health`.

## Endpoint

| Endpoint | Ý nghĩa |
|---|---|
| `GET /health` | Kiểm tra service và trạng thái đã cấu hình AI key |
| `GET /summary/overview?hours=24` | Tổng quan AQI, PM2.5, PM10 và cảnh báo |
| `GET /summary/aqi-trend?city=hanoi&hours=24` | Xu hướng AQI của thành phố |
| `GET /summary/particulate?city=hanoi&hours=24` | Phân tích PM2.5 và PM10 |
| `GET /summary/compare?hours=24` | So sánh các thành phố |
| `POST /ask` | Hỏi tự do bằng tiếng Việt |

Response summary có cấu trúc:

```json
{
  "mode": "ai",
  "scope": "overview",
  "city": "all",
  "time_range": "24h",
  "summary": "Nhận xét tiếng Việt",
  "highlights": [],
  "warnings": [],
  "data_updated_at": "2026-11-02T00:00:00Z",
  "generated_at": "2026-11-02T00:05:00Z",
  "response_time_ms": 123
}
```

`mode` có thể là `ai`, `data_summary` hoặc `fallback`. `data_summary` là thống kê thật khi không có key; `fallback` là thống kê thật khi API AI lỗi.

## Các panel AI trong Grafana

- `AI phân tích xu hướng AQI`: đặt cạnh biểu đồ AQI.
- `AI nhận xét bụi mịn`: đặt cạnh PM2.5/PM10.
- `AI nhận xét so sánh thành phố`: đặt cạnh biểu đồ thành phố.
- `AI tổng hợp dữ liệu hiện tại`: đặt cạnh bản đồ.
- `Trợ lý AI chất lượng không khí`: mở giao diện hỏi đáp tự do tại port 8090.

Các panel summary truy vấn InfluxDB, vì vậy không gọi OpenAI mỗi lần Grafana refresh.

## Cấu hình

```env
AI_API_KEY=điền-key-cục-bộ
AI_API_URL=https://api.openai.com/v1/chat/completions
AI_MODEL=gpt-4o-mini
AI_AUTOPUBLISH=true
AI_SUMMARY_REFRESH_SECONDS=600
```

Không commit `.env` và không đưa API key vào Grafana provisioning.

## Kịch bản demo

1. Chạy `.\start.ps1`.
2. Mở `http://localhost:3000` và chọn `Last 24 hours`.
3. Giải thích hàng KPI, biểu đồ AQI, PM2.5/PM10 và bản đồ ba lớp.
4. Chỉ ra các panel AI bên cạnh biểu đồ; kiểm tra `data_updated_at`.
5. Mở `http://localhost:8090` và hỏi: `Thành phố nào có PM2.5 cao nhất?`.
6. Tắt hoặc làm sai key trong môi trường test để trình bày fallback, không dùng dữ liệu giả.

