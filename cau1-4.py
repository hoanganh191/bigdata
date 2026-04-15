from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.4_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

# Hàm đọc và chuẩn hóa cột ID để tránh lỗi ClassCast
def read_standard(f):
    return spark.read.parquet(hdfs_path + f).select(F.col("PULocationID").cast(LongType()))

# Gộp dữ liệu 6 tháng
df_all = reduce(lambda df1, df2: df1.union(df2), [read_standard(f) for f in files])

print("\n" + "="*50)
print("KẾT QUẢ CÂU 1.4: TOP 20 ĐIỂM ĐÓN KHÁCH NHIỀU NHẤT")
print("="*50)

# Tính toán và hiển thị
top_20_pickup = df_all.groupBy("PULocationID") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(20)

top_20_pickup.show()

spark.stop()