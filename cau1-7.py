from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.7_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

def read_standard_1_7(f):
    # Đọc và chỉ lấy các cột thời gian cần thiết
    return spark.read.parquet(hdfs_path + f).select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID"
    )

# Gộp dữ liệu 6 tháng
df_all = reduce(lambda df1, df2: df1.union(df2), [read_standard_1_7(f) for f in files])

print("\n" + "="*70)
print("KẾT QUẢ CÂU 1.7: TOP 10 CHUYẾN ĐI CÓ THỜI LƯỢNG DÀI NHẤT (NHÓM 9)")
print("="*70)

# 1. Tính thời lượng chuyến đi (đơn vị: Giây)
# Chuyển đổi timestamp sang Unix time (giây) rồi trừ cho nhau
df_duration = df_all.withColumn(
    "duration_seconds", 
    F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
)

# 2. Chuyển đổi sang định dạng Giờ:Phút:Giây để báo cáo dễ nhìn hơn
df_final = df_duration.withColumn(
    "duration_hms",
    F.concat(
        F.floor(F.col("duration_seconds") / 3600), F.lit("h "),
        F.floor((F.col("duration_seconds") % 3600) / 60), F.lit("m "),
        F.col("duration_seconds") % 60, F.lit("s")
    )
)

# 3. Tìm 10 chuyến đi dài nhất
top_10_longest = df_final.orderBy(F.desc("duration_seconds")).limit(10)

# Hiển thị kết quả
top_10_longest.select(
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime", 
    "duration_hms", 
    "duration_seconds"
).show(truncate=False)

spark.stop()