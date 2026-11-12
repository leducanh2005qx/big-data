import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from dotenv import load_dotenv
from .observation import normalize_csv_aqi_row

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

try:
    # Code siêu tối giản, loại bỏ tính năng gom lô quá to gây nghẽn
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 4, 0),
        request_timeout_ms=60000, # Cho phép mạng độ trễ 60 giây
        max_block_ms=60000        # Chờ tối đa 60 giây để Kafka kịp tạo Topic
    )
    print("🚀 [HỆ THỐNG] Đã kết nối Kafka thành công! Đang lên đạn...")
except Exception as e:
    print(f"❌ [LỖI] Không thể kết nối Kafka: {e}")
    exit()

TOPIC_NAME = 'air_quality_data'
CSV_FILE_PATH = Path(__file__).resolve().parents[1] / 'data' / 'global_air_real.csv'

records_sent = 0
errors_caught = 0

try:
    while True:
        with CSV_FILE_PATH.open(mode='r', encoding='utf-8-sig', newline='') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    payload = normalize_csv_aqi_row(row)
                    
                    # Chờ tối đa 60 giây cho viên đạn đầu tiên (do Kafka tốn thời gian tạo Topic)
                    future = producer.send(TOPIC_NAME, value=payload)
                    future.get(timeout=60) 
                    records_sent += 1
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã đẩy -> {payload['city']} ({payload['country']}): PM2.5 AQI = {payload['value']}")
                    time.sleep(0.01)
                    
                except ValueError:
                    errors_caught += 1
                except KafkaTimeoutError:
                    print(f"\n❌ [LỖI MẠNG] Quá thời gian chờ ở bản ghi thứ {records_sent}. Kafka bị nghẽn!")
                    raise
            
        print(f"\n✅ Đã truyền xong 1 vòng: {records_sent} bản ghi hợp lệ. Lặp lại...\n")

except KeyboardInterrupt:
    print(f"\n🛑 Đã tắt. Tổng số dữ liệu đã truyền: {records_sent} sự kiện.")
    producer.close()
except FileNotFoundError:
    print(f"❌ [LỖI] Không tìm thấy file CSV tại {CSV_FILE_PATH}")