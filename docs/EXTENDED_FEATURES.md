# Các chức năng mở rộng

Service chạy tại `http://localhost:8090` và dùng dữ liệu thật trong InfluxDB.

## 1. Dự báo AQI

```powershell
Invoke-RestMethod "http://localhost:8090/forecast?city=hanoi&hours=168&horizon=24" | ConvertTo-Json -Depth 8
```

Đây là dự báo ước tính bằng xu hướng tuyến tính giảm dần trên chuỗi AQI theo giờ. Kết quả không phải số đo chắc chắn và API ghi rõ `is_estimate=true`.

## 2. Phát hiện bất thường

```powershell
Invoke-RestMethod "http://localhost:8090/anomalies?hours=168" | ConvertTo-Json -Depth 8
```

Hệ thống gom dữ liệu theo giờ, dùng median-MAD để giảm ảnh hưởng của các điểm cực trị và trả về thành phố, chỉ số, mức nền, điểm bất thường và hướng tăng/giảm.

## 3. Tư vấn chọn thành phố

```powershell
Invoke-RestMethod "http://localhost:8090/recommendations?hours=24&limit=5" | ConvertTo-Json -Depth 8
```

Điểm sạch tương đối hiện dùng AQI 60%, PM2.5 25% và PM10 15%. Thành phố không có dữ liệu sẽ không được xếp hạng. Đây là gợi ý theo dữ liệu, không phải tư vấn y tế.

Trong trang chat, có thể hỏi trực tiếp:

- `Dự báo AQI Hà Nội trong 24 giờ tới như thế nào?`
- `Có điểm bất thường hoặc tăng đột biến nào không?`
- `Tôi muốn đi du lịch, nên chọn thành phố nào?`

## 4. Cảnh báo email

Kiểm tra trạng thái:

```powershell
Invoke-RestMethod http://localhost:8090/email/status | ConvertTo-Json
```

Xem trước email mà không gửi:

```powershell
Invoke-RestMethod http://localhost:8090/email/preview | ConvertTo-Json -Depth 8
```

### Demo khi thuyết trình

Trong trang chat, bấm **Gửi thử email demo**. Mặc định nút chỉ tạo preview và gắn nhãn `[DEMO]`, không gửi ra ngoài. Nếu đã cấu hình SMTP và muốn gửi email demo thật trong buổi thuyết trình, đặt thêm:

```dotenv
ALERT_EMAIL_ENABLED=true
ALERT_EMAIL_DRY_RUN=false
ALERT_EMAIL_DEMO_SEND=true
```

Sau đó rebuild Docker. Lệnh tương đương:

```powershell
$body = '{"send":true}'
Invoke-RestMethod http://localhost:8090/email/demo -Method Post -ContentType 'application/json' -Body $body | ConvertTo-Json -Depth 8
```

Email demo luôn ghi rõ đây là dữ liệu trình diễn, không phải quan trắc thật. Không dùng sự kiện demo để kết luận tình hình môi trường.

Luồng email là:

```text
InfluxDB có chỉ số vượt ngưỡng
        -> AI service kiểm tra định kỳ
        -> SMTP_USERNAME/ALERT_EMAIL_FROM gửi email
        -> ALERT_EMAIL_TO nhận email
```

Ví dụ Gmail:

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

`SMTP_PASSWORD` phải là App Password của Gmail. Không gửi mật khẩu hoặc API key vào chat và không commit `.env` lên Git.

Ngưỡng vận hành:

```dotenv
ALERT_AQI_THRESHOLD=100
ALERT_PM25_THRESHOLD=35
ALERT_PM10_THRESHOLD=150
ALERT_EMAIL_COOLDOWN_SECONDS=3600
```

Giữ `ALERT_EMAIL_DRY_RUN=true` để tạo preview mà không gửi thật. Sau khi sửa `.env`, chạy:

```powershell
.\docker.ps1 rebuild
```

Chức năng email mặc định tắt để tránh gửi nhầm khi chưa cấu hình người gửi và người nhận.
