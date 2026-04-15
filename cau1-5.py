from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.5_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

# Hàm đọc và chuẩn hóa cột ID và số tiền
def read_standard_revenue(f):
    return spark.read.parquet(hdfs_path + f).select(
        F.col("DOLocationID").cast(LongType()),
        F.col("total_amount").cast(DoubleType())
    )

# Gộp dữ liệu 6 tháng
df_revenue = reduce(lambda df1, df2: df1.union(df2), [read_standard_revenue(f) for f in files])

print("\n" + "="*50)
print("KẾT QUẢ CÂU 1.5: TOP 20 ĐIỂM TRẢ KHÁCH DOANH THU CAO NHẤT")
print("="*50)

# Tính toán, định dạng số và hiển thị
top_20_revenue = df_revenue.groupBy("DOLocationID") \
    .agg(F.sum("total_amount").alias("Raw_Revenue")) \
    .withColumn("Total_Revenue", F.format_number("Raw_Revenue", 2)) \
    .orderBy(F.desc("Raw_Revenue")) \
    .select("DOLocationID", "Total_Revenue") \
    .limit(20)

top_20_revenue.show(truncate=False)

spark.stop()