from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import subprocess
import sys

# Khởi tạo SparkSession chỉ cho chuẩn hóa
spark = SparkSession.builder \
    .appName("NYC_Taxi_Data_Preparation") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/taxi"
normalized_path = "hdfs://localhost:9000/data_taxi_normalized"

# Cấu trúc output sau chuẩn hóa:
# /data_taxi_normalized/
#   ├── 03_yellow_tripdata_2023-01.parquet/ (thư mục con)
#   ├── 03_yellow_tripdata_2023-02.parquet/ (thư mục con)
#   └── ...

print("[1] Tìm kiếm file parquet...")
sys.stdout.flush()

result = subprocess.run(["hdfs", "dfs", "-ls", data_path], capture_output=True, text=True)
parquet_files = [line.split()[-1] for line in result.stdout.strip().split("\n") if line and not line.startswith("Found") and len(line.split()) >= 8 and line.split()[-1].endswith('.parquet')]

print(f"[2] Tìm thấy {len(parquet_files)} file parquet")
sys.stdout.flush()

if not parquet_files:
    raise ValueError(f"Không tìm thấy file parquet trong HDFS: {data_path}")

# Xóa normalized_path cũ
print("[3] Xóa thư mục chuẩn hóa cũ (nếu có)...")
subprocess.run(["hdfs", "dfs", "-rm", "-r", normalized_path], capture_output=True, text=True)
sys.stdout.flush()

# Chuẩn hóa schema từng file Parquet thành folder normalized
print("[4] Chuẩn hóa dữ liệu...")
for i, f in enumerate(parquet_files, 1):
    filename = f.split("/")[-1]
    print(f"   [{i}/{len(parquet_files)}] Xử lý {filename}...", end="", flush=True)
    try:
        df = spark.read.parquet(f)
        normalized_df = df.select(
            F.col("PULocationID").cast("long").alias("PULocationID"),
            F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime")
        )
        normalized_target = f"{normalized_path}/{filename}"
        normalized_df.write.mode("overwrite").parquet(normalized_target)
        print(" ✓")
        sys.stdout.flush()
    except Exception as e:
        print(f" ✗ Error: {e}")
        sys.stdout.flush()

print("\n[5] Chuẩn hóa xong!")
print(f"Dữ liệu chuẩn hóa nằm tại: {normalized_path}/")
subprocess.run(["hdfs", "dfs", "-ls", normalized_path], capture_output=True)
sys.stdout.flush()

spark.stop()
