from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("NYC_Taxi_Streaming_Growth_Nhom9") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.parquet.enableDecimalConversion", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sử dụng HDFS path
data_path = "hdfs://localhost:9000/user/nhom9/data_taxi"

# Đọc tất cả file parquet từ HDFS folder
# Chỉ lấy các cột cần thiết để tránh schema mismatch
from functools import reduce
from pyspark.sql import DataFrame

# Đọc từng file và cast schema để đảm bảo consistent
def read_and_standardize(file_path):
    df = spark.read.parquet(file_path)
    # Chỉ lấy 2 cột cần thiết
    return df.select(
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("tpep_pickup_datetime").alias("tpep_pickup_datetime")
    )

# Lấy danh sách file từ HDFS
import subprocess
result = subprocess.run(["hdfs", "dfs", "-ls", data_path], capture_output=True, text=True)
parquet_files = []
for line in result.stdout.strip().split('\n'):
    if line and not line.startswith('Found'):
        parts = line.split()
        if len(parts) >= 8:
            file_path = parts[-1]
            if file_path.endswith('.parquet'):
                parquet_files.append(file_path)

print(f"Found {len(parquet_files)} parquet files on HDFS")

# Đọc và union tất cả file
dfs = [read_and_standardize(f) for f in parquet_files]
taxi_df = reduce(DataFrame.unionByName, dfs)

# Tiền xử lý
processed_df = taxi_df.select(
    F.col("PULocationID"),
    F.to_date(F.col("tpep_pickup_datetime")).alias("trip_date")
).filter("trip_date IS NOT NULL")

# Gom nhóm theo ngày
daily_counts = processed_df.groupBy("trip_date", "PULocationID") \
    .agg(F.count("*").alias("trip_count")) \
    .orderBy("trip_date")

# Tính growth rate
window_spec = Window.partitionBy("PULocationID").orderBy("trip_date")

growth_df = daily_counts.withColumn("prev_trip_count", F.lag("trip_count").over(window_spec)) \
    .withColumn("growth_rate", 
        F.when(F.col("prev_trip_count") > 0, 
               (F.col("trip_count") - F.col("prev_trip_count")) / F.col("prev_trip_count"))
        .otherwise(0)
    )

# Hiển thị kết quả
print("\n=== NYC Taxi Growth Rate Analysis ===")
growth_df.orderBy(F.desc("growth_rate")) \
    .select("PULocationID", "trip_date", "trip_count", "growth_rate") \
    .limit(10) \
    .show()

# Lưu kết quả (tùy chọn) - lưu vào HDFS
growth_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/nhom9/output_taxi_growth")