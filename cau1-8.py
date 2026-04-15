from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.8_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

def read_standard_1_8(f):
    return spark.read.parquet(hdfs_path + f).select(
        F.col("tpep_pickup_datetime"),
        F.col("total_amount").cast(DoubleType())
    )

# Gộp dữ liệu 6 tháng
df_all = reduce(lambda df1, df2: df1.union(df2), [read_standard_1_8(f) for f in files])

print("\n" + "="*60)
print("KẾT QUẢ CÂU 1.8: THỐNG KÊ DOANH THU THEO KHUNG GIỜ (NHÓM 9)")
print("="*60)

# 1. Trích xuất giờ từ thời điểm đón khách
df_with_hour = df_all.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))

# 2. Phân loại khung giờ bằng hàm WHEN...OTHERWISE
df_time_slots = df_with_hour.withColumn(
    "time_slot",
    F.when((F.col("pickup_hour") >= 0) & (F.col("pickup_hour") <= 5), "0-5h")
     .when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") <= 11), "6-11h")
     .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") <= 17), "12-17h")
     .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") <= 23), "18-23h")
     .otherwise("Unknown")
)

# 3. Gom nhóm theo khung giờ và tính tổng doanh thu
revenue_by_slot = df_time_slots.groupBy("time_slot") \
    .agg(F.round(F.sum("total_amount"), 2).alias("Total_Revenue")) \
    .withColumn("Formatted_Revenue", F.format_number("Total_Revenue", 2)) \
    .orderBy("time_slot") # Sắp xếp theo tên khung giờ cho dễ nhìn

# Hiển thị kết quả
revenue_by_slot.select("time_slot", "Formatted_Revenue").show()

spark.stop()