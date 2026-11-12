import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

import pyspark

project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

# Bơm môi trường giả lập Hadoop cho Windows
hadoop_path = str(project_root / 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_path
os.environ['hadoop.home.dir'] = hadoop_path
# Ép Windows nhận diện file hadoop.dll trong system PATH
os.environ['PATH'] = hadoop_path + r'\bin;' + os.environ.get('PATH', '')
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark_driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "1g")
spark_local_cores = int(os.getenv("SPARK_LOCAL_CORES", "2"))
if spark_local_cores < 1:
    raise ValueError("SPARK_LOCAL_CORES must be at least 1")
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {spark_driver_memory} pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, window
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, TimestampType
from influxdb import InfluxDBClient
from collection_status import to_influx_collection_status_point
from quarantine import persist_jsonl_quarantine

influxdb_host = os.getenv("INFLUXDB_HOST", "127.0.0.1")
influxdb_port = int(os.getenv("INFLUXDB_PORT", "8086"))
influxdb_username = os.getenv("INFLUXDB_ADMIN_USER")
influxdb_password = os.getenv("INFLUXDB_ADMIN_PASSWORD")
influxdb_database = os.getenv("INFLUXDB_DATABASE", "air_quality")
raw_retention_policy = os.getenv("INFLUXDB_RAW_RETENTION_POLICY", "raw_7d")
aggregate_retention_policy = os.getenv("INFLUXDB_AGGREGATE_RETENTION_POLICY", "aggregates_30d")
checkpoint_root = Path(os.getenv("SPARK_CHECKPOINT_DIR", str(project_root / ".spark_checkpoints")))
ivy_cache_dir = Path(os.getenv("SPARK_IVY_DIR", str(project_root / ".ivy2"))).resolve()
ivy_cache_dir.mkdir(parents=True, exist_ok=True)
status_topic = os.getenv("KAFKA_STATUS_TOPIC", "air_quality_collection_status")
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

if not influxdb_username or not influxdb_password:
    raise RuntimeError("Set INFLUXDB_ADMIN_USER and INFLUXDB_ADMIN_PASSWORD in the project .env file")

# 1. Định nghĩa cấu trúc sự kiện demo do producer phát ra
attribution_schema = StructType([
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("license", StringType(), True)
])

schema = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("data_mode", StringType(), True),
    StructField("source_record_id", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("observed_at", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("pollutant", StringType(), True),
    StructField("metric_type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("category", StringType(), True),
    StructField("aqi_standard", StringType(), True),
    StructField("quality_flags", ArrayType(StringType()), True),
    StructField("attributions", ArrayType(attribution_schema), True)
])

collection_status_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("status_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("poll_started_at", StringType(), True),
    StructField("poll_completed_at", StringType(), True),
    StructField("records_published", DoubleType(), True),
    StructField("records_rejected", DoubleType(), True),
    StructField("error", StringType(), True)
])

# 2. Hàm xử lý từng lô dữ liệu (Micro-batch) và Ghi vào InfluxDB
def process_and_write_to_influx(df, epoch_id):
    client = InfluxDBClient(
        host=influxdb_host,
        port=influxdb_port,
        username=influxdb_username,
        password=influxdb_password,
        database=influxdb_database
    )
    points = []
    written = 0
    try:
        for row in df.toLocalIterator():
            if row.value is None or row.metric_type is None or row.unit is None or row.ingested_at is None:
                continue

            fields = {
                "value": float(row.value),
                "event_id": row.event_id or "unknown",
                "ingested_at": row.ingested_at.isoformat(),
                "quality_flags": json.dumps(row.quality_flags or []),
                "attributions": json.dumps([
                    {
                        "name": attribution.name,
                        "url": attribution.url,
                        "license": attribution.license
                    }
                    for attribution in row.attributions or []
                ])
            }
            if row.category is not None:
                fields["category"] = row.category
            if row.observed_at is not None:
                fields["observed_at"] = row.observed_at.isoformat()
            if row.latitude is not None:
                fields["latitude"] = float(row.latitude)
            if row.longitude is not None:
                fields["longitude"] = float(row.longitude)
            if row.aqi_standard is not None:
                fields["aqi_standard"] = row.aqi_standard
            if row.source_record_id is not None:
                fields["source_record_id"] = row.source_record_id
            if row.metric_type == "aqi":
                fields["aqi_above_demo_threshold"] = int(row.value > 150.0)

            tags = {
                "source": row.source or "unknown",
                "data_mode": row.data_mode or "unknown",
                "pollutant": row.pollutant or "unknown",
                "metric_type": row.metric_type,
                "unit": row.unit
            }
            for tag_name in ("country", "city", "station_id", "aqi_standard"):
                tag_value = getattr(row, tag_name)
                if tag_value is not None:
                    tags[tag_name] = tag_value

            points.append({
                "measurement": "air_quality_observation",
                "tags": tags,
                "time": (row.observed_at or row.ingested_at).isoformat(),
                "fields": fields
            })

            if len(points) >= 500:
                if not client.write_points(points, retention_policy=raw_retention_policy):
                    raise RuntimeError("InfluxDB rejected a micro-batch of points")
                written += len(points)
                points.clear()

        if points:
            if not client.write_points(points, retention_policy=raw_retention_policy):
                raise RuntimeError("InfluxDB rejected a micro-batch of points")
            written += len(points)
    finally:
        client.close()

    if written:
        print(f"⚡ [PySpark] Đã xử lý & ghi thành công {written} bản ghi vào InfluxDB (Lô: {epoch_id})")


def write_hourly_aggregates(df, epoch_id):
    client = InfluxDBClient(
        host=influxdb_host,
        port=influxdb_port,
        username=influxdb_username,
        password=influxdb_password,
        database=influxdb_database
    )
    points = []
    written = 0
    try:
        for row in df.toLocalIterator():
            if row.hour is None or row.average_value is None:
                continue

            tags = {
                "source": row.source or "unknown",
                "data_mode": row.data_mode or "unknown",
                "pollutant": row.pollutant or "unknown",
                "metric_type": row.metric_type or "unknown",
                "unit": row.unit or "unknown"
            }
            for tag_name in ("country", "city", "station_id", "aqi_standard"):
                tag_value = getattr(row, tag_name)
                if tag_value is not None:
                    tags[tag_name] = tag_value

            points.append({
                "measurement": "air_quality_hourly",
                "tags": tags,
                "time": row.hour.start.isoformat(),
                "fields": {
                    "average_value": float(row.average_value),
                    "sample_count": int(row.sample_count),
                    "window_end": row.hour.end.isoformat()
                }
            })

            if len(points) >= 500:
                if not client.write_points(points, retention_policy=aggregate_retention_policy):
                    raise RuntimeError("InfluxDB rejected an hourly aggregate batch")
                written += len(points)
                points.clear()

        if points:
            if not client.write_points(points, retention_policy=aggregate_retention_policy):
                raise RuntimeError("InfluxDB rejected an hourly aggregate batch")
            written += len(points)
    finally:
        client.close()

    if written:
        print(f"[PySpark] Wrote {written} hourly aggregates (batch: {epoch_id})")


def persist_invalid_batch(df, epoch_id):
    raw_records = (row.raw_json for row in df.toLocalIterator() if row.raw_json)
    quarantined = persist_jsonl_quarantine(
        raw_records,
        epoch_id,
        project_root / "data" / "quarantine",
    )
    if quarantined:
        print(f"[PySpark] Quarantined {quarantined} invalid events from batch {epoch_id}")


def write_collection_status_batch(df, epoch_id):
    client = InfluxDBClient(
        host=influxdb_host,
        port=influxdb_port,
        username=influxdb_username,
        password=influxdb_password,
        database=influxdb_database
    )
    points = []
    try:
        for row in df.toLocalIterator():
            try:
                points.append(to_influx_collection_status_point(row.asDict(recursive=True)))
            except ValueError as error:
                print(f"[PySpark] Ignoring invalid collection-status event in batch {epoch_id}: {error}")
        if points and not client.write_points(points, retention_policy=raw_retention_policy):
            raise RuntimeError("InfluxDB rejected a collection-status batch")
    finally:
        client.close()

# 3. Khởi tạo Trái tim Spark (Tự động thích ứng phiên bản)
print("🚀 Đang khởi động Apache Spark Engine...")
spark_version = pyspark.__version__

# Bắt buộc dùng Scala 2.13 để fix lỗi WrappedArray trên các bản PySpark mới
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"
print(f"📦 Đang nạp thư viện Kafka tương thích: {kafka_package}")

spark = SparkSession.builder \
    .appName("AirQuality_Realtime_Engine") \
    .master(f"local[{spark_local_cores}]") \
    .config("spark.driver.memory", spark_driver_memory) \
    .config("spark.jars.ivy", str(ivy_cache_dir)) \
    .config("spark.jars.packages", kafka_package) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Tắt bớt log rác để dễ nhìn
ready_file = Path(os.getenv("SPARK_READY_FILE", "/tmp/airquality-spark-ready"))
ready_file.parent.mkdir(parents=True, exist_ok=True)
ready_file.touch()

# 4. Đọc luồng dữ liệu liên tục từ Kafka
print("📡 Đang kết nối vào dòng chảy Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "air_quality_data") \
    .option("startingOffsets", "earliest") \
    .load()

# 5. Parse và tách sự kiện hợp lệ khỏi payload cần quarantine
parsed_records = df.selectExpr("CAST(value AS STRING) AS raw_json") \
    .select("raw_json", from_json(col("raw_json"), schema).alias("data"))

invalid_condition = (
    col("data").isNull()
    | col("data.event_id").isNull()
    | col("data.source").isNull()
    | col("data.value").isNull()
    | col("data.metric_type").isNull()
    | col("data.unit").isNull()
    | col("data.ingested_at").isNull()
)
invalid_events = parsed_records.filter(invalid_condition).select("raw_json")
parsed_df = parsed_records.filter(~invalid_condition).select("data.*")

quarantine_query = invalid_events.writeStream \
    .outputMode("append") \
    .foreachBatch(persist_invalid_batch) \
    .option("checkpointLocation", str(checkpoint_root / "quarantine")) \
    .start()

# 6. Kích hoạt dòng chảy và gọi hàm xử lý
print("🔥 HỆ THỐNG PHÂN TÍCH ĐÃ SẴN SÀNG CHẠY! Đang chờ dữ liệu...")
query = parsed_df.writeStream \
    .foreachBatch(process_and_write_to_influx) \
    .option("checkpointLocation", str(checkpoint_root / "raw")) \
    .start()

hourly_aggregates = parsed_df \
    .filter(col("observed_at").isNotNull() & col("value").isNotNull()) \
    .withWatermark("observed_at", "2 hours") \
    .groupBy(
        window(col("observed_at"), "1 hour").alias("hour"),
        "source",
        "data_mode",
        "country",
        "city",
        "station_id",
        "pollutant",
        "metric_type",
        "unit",
        "aqi_standard"
    ) \
    .agg(
        avg("value").alias("average_value"),
        count("value").alias("sample_count")
    )

aggregate_query = hourly_aggregates.writeStream \
    .outputMode("append") \
    .foreachBatch(write_hourly_aggregates) \
    .option("checkpointLocation", str(checkpoint_root / "hourly")) \
    .start()

status_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", status_topic) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_status = status_stream.selectExpr("CAST(value AS STRING) AS raw_json") \
    .select(from_json(col("raw_json"), collection_status_schema).alias("status")) \
    .select("status.*")

status_query = parsed_status.writeStream \
    .outputMode("append") \
    .foreachBatch(write_collection_status_batch) \
    .option("checkpointLocation", str(checkpoint_root / "status")) \
    .start()

spark.streams.awaitAnyTermination()
