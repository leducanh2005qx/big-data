# Báo cáo tổng quan dự án giám sát chất lượng không khí

**Ngày lập:** 12/11/2026  
**Phạm vi:** Tổng hợp mã nguồn, tài liệu và các thay đổi đã thực hiện trong workspace đến ngày lập báo cáo.

## 1. Mục tiêu dự án

Dự án xây dựng hệ thống thu thập, xử lý, lưu trữ và trực quan hóa dữ liệu chất lượng không khí. Hệ thống tách biệt chỉ số AQI với nồng độ chất ô nhiễm, giữ thông tin nguồn dữ liệu, đơn vị, địa điểm và thời điểm đo để tránh so sánh sai loại dữ liệu.

## 2. Kiến trúc và luồng dữ liệu

```text
Open-Meteo / OpenAQ / dữ liệu CSV lịch sử
                 ↓
       Collector chuẩn hóa sự kiện
                 ↓
       Kafka: air_quality_data
                 ↓
        Spark Structured Streaming
          ↙          ↓           ↘
   InfluxDB raw   hourly rollup   quarantine JSONL
          ↓          ↓
                Grafana
          ↕                  
       Dịch vụ AI hỏi đáp và phân tích
```

Các thành phần chính:

- **Kafka** tiếp nhận sự kiện quan trắc và trạng thái thu thập.
- **Spark Structured Streaming** kiểm tra, chuẩn hóa và ghi dữ liệu vào InfluxDB; dữ liệu sự kiện lỗi được đưa vào vùng quarantine.
- **InfluxDB** lưu dữ liệu thô, dữ liệu tổng hợp theo giờ và trạng thái của collector.
- **Grafana** hiển thị dashboard, bản đồ, biểu đồ và cảnh báo demo.
- **Dịch vụ AI** đọc số liệu từ InfluxDB, trả lời câu hỏi tiếng Việt và cung cấp phân tích dựa trên dữ liệu hiện có.
- **Docker Compose** khởi chạy các dịch vụ. MySQL được cấu hình như thành phần tùy chọn cho metadata.

## 3. Nguồn dữ liệu và collector

### Open-Meteo

Collector gọi Air Quality API, chuẩn hóa AQI và nồng độ PM2.5, PM10, CO, NO2, SO2, O3 rồi gửi sự kiện vào Kafka. Dữ liệu được gắn nhãn `model_estimate`, tức là kết quả mô hình, không phải số đo trực tiếp từ trạm.

Trước lần cập nhật gần đây, `.env` có 34 điểm lấy mẫu đại diện tại Việt Nam. Đã thêm điểm Nam Định, nâng cấu hình lên 35 điểm. Đây là các tọa độ đại diện; dữ liệu không bao phủ từng xã/phường. Collector chạy liên tục, mặc định lấy dữ liệu mỗi 600 giây.

### OpenAQ

Adapter OpenAQ v3 lấy thông tin địa điểm, cảm biến và số đo mới nhất; có xử lý retry cho lỗi tạm thời, lưu bản phản hồi thô và gửi trạng thái từng địa điểm. Thu thập dữ liệu trực tiếp cần API key và danh sách location ID hợp lệ.

### CSV lịch sử và dữ liệu demo

- CSV được phát lại để minh họa pipeline; thời điểm ingest không được xem là thời điểm quan trắc.
- Sự kiện kiểm thử tổng hợp được gắn nhãn riêng, không được coi là dữ liệu môi trường thực.

## 4. Chuẩn dữ liệu và xử lý

Schema sự kiện nằm tại `schemas/air_quality_observation.schema.json`. Sự kiện bao gồm nguồn, chế độ dữ liệu, thành phố, tọa độ, `observed_at`, `ingested_at`, chất ô nhiễm, loại chỉ số, giá trị, đơn vị, chuẩn AQI và thông tin quy thuộc nguồn.

Các phân biệt được giữ trong pipeline:

- `metric_type=aqi` dùng đơn vị `AQI index`; `metric_type=concentration` giữ đơn vị nồng độ.
- Không suy ra PM2.5 từ AQI và không tự điền thời điểm quan trắc còn thiếu.
- Dữ liệu thô có retention 7 ngày; dữ liệu tổng hợp theo giờ có retention 30 ngày.
- Tổng hợp theo giờ sử dụng event time và watermark 2 giờ.
- Collector phát trạng thái hoàn tất poll và số bản ghi; đây là tín hiệu độ mới của nguồn, không đồng nghĩa lần poll nào cũng có phép đo mới.
- Payload lỗi được cách ly thành JSONL để điều tra.

## 5. Dashboard Grafana

Dashboard được provision từ `grafana/dashboards/air-quality-overview.json`, gồm các panel chính:

- AQI, PM2.5, PM10 hiện tại và AQI cao nhất.
- Diễn biến AQI, PM2.5, PM10 và các khí CO, NO2, SO2, O3.
- So sánh PM2.5 theo thành phố, bản đồ AQI/nồng độ bụi.
- Các bảng tổng hợp do AI tạo, trạng thái collector và chú thích chỉ số.
- Bộ lọc quốc gia, thành phố, nguồn dữ liệu và khoảng thời gian.

Panel PM2.5 hiện được đặt lại ở dạng biểu đồ cột ngang. Khung panel được nới rộng và cao để giảm chồng lấn nhãn; truy vấn áp dụng bộ lọc thành phố hiện hành. Biểu đồ cột không có thanh cuộn riêng trong cấu hình hiện tại.

## 6. Trợ lý AI và phân tích

Dịch vụ chạy tại cổng 8090. Các chức năng hiện có:

- Hỏi đáp tiếng Việt theo ngữ cảnh dashboard và khoảng thời gian.
- Trả lời AQI/PM2.5/PM10 theo thành phố; so sánh đúng các địa danh người dùng nêu khi có dữ liệu.
- Gợi ý thành phố theo điểm sạch tương đối (AQI 60%, PM2.5 25%, PM10 15%). Đây là xếp hạng từ dữ liệu có sẵn, không phải tư vấn y tế.
- Dự báo AQI 24 giờ dựa trên xu hướng lịch sử; kết quả là ước tính.
- Phát hiện bất thường bằng median-MAD.
- Tạo summary, cache theo dấu vân tay dữ liệu và ghi summary vào InfluxDB để Grafana đọc.
- Xem trước email cảnh báo và gửi demo khi cấu hình cho phép.

### Sửa lỗi hỏi đáp mới nhất

Đã sửa chuẩn hóa chữ tiếng Việt để nhận diện được địa danh có dấu và không dấu, bổ sung alias Nam Định/Thanh Hóa. Khi câu hỏi có từ hai địa danh, trợ lý bỏ qua nhánh gợi ý du lịch toàn cục, chỉ so sánh các địa danh đó và không để câu trả lời sinh tự do của mô hình thay thế kết quả đã tính từ dữ liệu. Kiểm tra trực tiếp câu “So sánh thành phố Thanh Hóa và Nam Định” không còn trả nhầm Hưng Yên.

Đã thêm Nam Định vào cấu hình Open-Meteo và khởi động lại dịch vụ AI cùng collector. Lần kiểm tra sau cập nhật có dữ liệu Thanh Hóa nhưng chưa có bản ghi Nam Định; trợ lý hiện báo rõ thiếu dữ liệu thay vì tự điền hoặc chuyển sang thành phố khác. Cần xác nhận collector tiếp tục chạy và chờ lần ghi nhận Nam Định đầu tiên.

## 7. Cảnh báo và email

Grafana có quy tắc AQI demo và contact point webhook. Webhook cần URL hợp lệ để gửi thông báo thật. Dịch vụ AI hỗ trợ kiểm tra cấu hình email, tạo preview và email demo; mặc định chế độ demo không gửi ra ngoài. Các ngưỡng cảnh báo là cấu hình vận hành của dự án, không mặc định là ngưỡng y tế chính thức.

## 8. Kiểm tra đã thực hiện

- 17 kiểm thử liên quan đến AI và Open-Meteo collector đã chạy thành công sau các cập nhật gần nhất.
- Đã kiểm tra trực tiếp API `/ask` với câu so sánh Thanh Hóa và Nam Định; câu trả lời chỉ đề cập hai địa danh đó và báo Nam Định chưa có dữ liệu.
- Dịch vụ AI trả trạng thái `/health` là `ok` sau khi khởi động lại.
- Cấu hình collector đã phân tích được 35 địa điểm và có Nam Định.
- JSON dashboard đã được kiểm tra hợp lệ sau các chỉnh sửa panel PM2.5.

## 9. Giới hạn và việc còn lại

- 35 điểm là điểm đại diện theo cấu hình, không phải dữ liệu ở mọi xã/phường hoặc mọi trạm quan trắc.
- Nam Định chưa có số đo trong InfluxDB tại thời điểm kiểm tra; chưa thể kết luận so sánh AQI thực tế giữa Thanh Hóa và Nam Định cho đến khi collector ghi nhận dữ liệu.
- Open-Meteo cung cấp ước tính mô hình; không nên trình bày như phép đo tại trạm.
- OpenAQ cần API key và location ID do người vận hành cấu hình; độ phủ không đồng đều.
- Webhook thật, kiểm thử hết hạn retention, benchmark throughput/latency có kiểm soát và xác nhận CI trên repository còn cần thực hiện theo tài liệu bàn giao.
- Gợi ý du lịch hiện chỉ xếp hạng theo chất lượng không khí có dữ liệu; chưa xét giá cả, điểm tham quan, thời tiết du lịch hay giao thông.

## 10. Tài liệu và mã nguồn tham khảo

- `README.md`: cài đặt, chạy stack, nguồn dữ liệu và lệnh vận hành.
- `docs/architecture.md`: kiến trúc và luồng dữ liệu.
- `docs/AI_DASHBOARD_V2.md`, `docs/EXTENDED_FEATURES.md`: dịch vụ AI và tính năng mở rộng.
- `docs/FINAL_HANDOFF.md`, `docs/PROJECT_REPORT.md`: trạng thái bàn giao và kiểm chứng trước đó.
- `producer/open_meteo_collector.py`, `producer/openaq_collector.py`: collector nguồn dữ liệu.
- `spark_jobs/stream_processor.py`: xử lý streaming.
- `scripts/air_quality_ai.py`, `scripts/air_quality_features.py`: hỏi đáp và phân tích.
- `grafana/dashboards/air-quality-overview.json`: dashboard.
- `schemas/`: hợp đồng dữ liệu JSON Schema.
