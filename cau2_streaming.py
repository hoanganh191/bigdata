from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType, TimestampType
import subprocess
import sys

# Khởi tạo SparkSession cho streaming
spark = SparkSession.builder \
    .appName("NYC_Taxi_Streaming_Growth") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Đọc từ tất cả các thư mục con của 2023
# Wildcard (*) sẽ match: 01, 02, 03, ..., 12
normalized_path = "hdfs://localhost:9000/data_taxi_normalized/03_yellow_tripdata_2023-*/"

print("="*80)
print("KHỞI TẠO STREAMING TỪ DỮ LIỆU ĐÃ CHUẨN HÓA")
print("="*80)

# Xóa checkpoint cũ
checkpoint_path = "/tmp/checkpoint_taxi_streaming"
subprocess.run(["rm", "-rf", checkpoint_path], capture_output=True, text=True)

streaming_schema = StructType([
    StructField("PULocationID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True)
])

print(f"\n[1] Đọc dữ liệu từ: {normalized_path}")
print("[2] Trigger: 3 giây")
print("[3] maxFilesPerTrigger: 2\n")
sys.stdout.flush()

streaming_df = spark.readStream \
    .schema(streaming_schema) \
    .option("maxFilesPerTrigger", 2) \
    .parquet(normalized_path)

processed_df = streaming_df.select(
    F.col("PULocationID"),
    F.to_date(F.col("tpep_pickup_datetime")).alias("trip_date")
).filter("trip_date IS NOT NULL")

# Chỉ giữ ngày hợp lệ trong năm 2023
processed_df = processed_df.filter(
    (F.col("trip_date") >= F.lit("2023-01-01")) &
    (F.col("trip_date") <= F.lit("2023-12-31"))
)

daily_counts = processed_df.groupBy("trip_date", "PULocationID") \
    .agg(F.count("*").alias("trip_count"))

batch_count = [0]

def foreach_batch(batch_df, epoch_id):
    """Xử lý từng batch: hiển thị top 5 location theo trip count"""
    batch_count[0] += 1
    
    if batch_df.rdd.isEmpty():
        print(f"[Batch {epoch_id}] Không có dữ liệu")
        sys.stdout.flush()
        return

    rows_count = batch_df.count()
    print(f"\n{'='*80}")
    print(f"[BATCH {epoch_id}] Xử lý {rows_count} dòng dữ liệu")
    print(f"{'='*80}")
    sys.stdout.flush()

    # Xếp hạng top 5 mỗi ngày theo trip count
    rank_window = Window.partitionBy("trip_date").orderBy(F.desc("trip_count"))
    top5_df = batch_df.withColumn("rank", F.row_number().over(rank_window)) \
        .filter(F.col("rank") <= 5) \
        .select(
            F.col("trip_date"),
            F.col("PULocationID"),
            F.col("trip_count")
        )

    if top5_df.rdd.isEmpty():
        print("Không có dữ liệu Top 5 để hiển thị")
        sys.stdout.flush()
        return

    # Sắp xếp và hiển thị
    rows = top5_df.orderBy("trip_date", F.desc("trip_count")).collect()

    current_date = None
    rank = 1

    for row in rows:
        if row.trip_date != current_date:
            if current_date is not None:
                print()
            print(f"\n Ngày: {row.trip_date}")
            print(f"{'Rank':<6} {'PULocationID':<15} {'trip_count':<15}")
            print("-" * 40)
            current_date = row.trip_date
            rank = 1

        print(f"{rank:<6} {row.PULocationID:<15} {row.trip_count:<15}")
        rank += 1

    print()
    sys.stdout.flush()


query = daily_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(foreach_batch) \
    .trigger(processingTime="3 seconds") \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print("\n[STATUS] Streaming đang chạy...")
print("[*] Nhấn Ctrl+C để dừng\n")
sys.stdout.flush()

try:
    query.awaitTermination(timeout=180)
except KeyboardInterrupt:
    print("\n[*] Dừng streaming...")
    query.stop()

print(f"\n{'='*80}")
print(f"✓ Streaming kết thúc (tổng {batch_count[0]} batch)")
print(f"{'='*80}")
sys.stdout.flush()

spark.stop()
