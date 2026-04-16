from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType, TimestampType
import subprocess

spark = SparkSession.builder \
    .appName("NYC_Taxi_Streaming_Growth_Nhom9") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/user/nhom9/data_taxi"
normalized_path = "hdfs://localhost:9000/user/nhom9/data_taxi_normalized"

result = subprocess.run(["hdfs", "dfs", "-ls", data_path], capture_output=True, text=True)
parquet_files = [line.split()[-1] for line in result.stdout.strip().split("\n") if line and not line.startswith("Found") and len(line.split()) >= 8 and line.split()[-1].endswith('.parquet')]

if not parquet_files:
    raise ValueError(f"Không tìm thấy file parquet trong HDFS: {data_path}")

# Chuẩn hóa schema từng file Parquet thành folder normalized
for f in parquet_files:
    df = spark.read.parquet(f)
    normalized_df = df.select(
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime")
    )
    basename = f.split("/")[-1]
    normalized_target = f"{normalized_path}/{basename}"
    normalized_df.write.mode("overwrite").parquet(normalized_target)

streaming_schema = StructType([
    StructField("PULocationID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True)
])

streaming_df = spark.readStream \
    .schema(streaming_schema) \
    .option("maxFilesPerTrigger", 1) \
    .parquet(normalized_path)

processed_df = streaming_df.select(
    F.col("PULocationID"),
    F.to_date(F.col("tpep_pickup_datetime")).alias("trip_date")
).filter("trip_date IS NOT NULL")

# Chỉ giữ ngày hợp lệ trong năm 2023 để loại các giá trị sai ngày
processed_df = processed_df.filter(
    (F.col("trip_date") >= F.lit("2023-01-01")) &
    (F.col("trip_date") <= F.lit("2023-12-31"))
)

daily_counts = processed_df.groupBy("trip_date", "PULocationID") \
    .agg(F.count("*").alias("trip_count"))

window_spec = Window.partitionBy("PULocationID").orderBy("trip_date")

def foreach_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {epoch_id}: empty")
        return

    growth_df = batch_df.withColumn("prev_trip_count", F.lag("trip_count").over(window_spec)) \
        .withColumn("growth_rate", 
            F.when(F.col("prev_trip_count") > 0, 
                   (F.col("trip_count") - F.col("prev_trip_count")) / F.col("prev_trip_count"))
            .otherwise(0)
        )

    rank_window = Window.partitionBy("trip_date").orderBy(F.desc("growth_rate"))
    top5_daily = growth_df.withColumn("rank", F.row_number().over(rank_window)) \
        .filter(F.col("rank") <= 5) \
        .select("PULocationID", "trip_date", "trip_count", "growth_rate") \
        .orderBy("trip_date", F.desc("growth_rate"))

    print(f"\n=== Batch {epoch_id}: Top 5 vùng tăng mạnh nhất theo ngày (2023) ===")
    rows = top5_daily.orderBy("trip_date", "growth_rate", "PULocationID").collect()
    current_date = None
    for row in rows:
        if row.trip_date != current_date:
            current_date = row.trip_date
            print(f"\nNgày: {current_date}")
            print("PULocationID | trip_count | growth_rate")
        print(f"{row.PULocationID:12} | {row.trip_count:10} | {row.growth_rate}")


query = daily_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
