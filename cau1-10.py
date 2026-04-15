from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.10_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

def read_standard_1_10(f):
    # Chọn các cột cần thiết và ép kiểu Double để tính toán chính xác
    return spark.read.parquet(hdfs_path + f).select(
        F.col("tip_amount").cast(DoubleType()),
        F.col("fare_amount").cast(DoubleType()),
        F.col("PULocationID"),
        F.col("DOLocationID")
    )

# Gộp dữ liệu 6 tháng
df_all = reduce(lambda df1, df2: df1.union(df2), [read_standard_1_10(f) for f in files])

print("\n" + "="*70)
print("KẾT QUẢ CÂU 1.10: TOP 20 CHUYẾN ĐI CÓ TỶ LỆ TIP CAO NHẤT (NHÓM 9)")
print("="*70)

# 1. Lọc điều kiện fare_amount > 0 trước khi chia
df_filtered = df_all.filter(F.col("fare_amount") > 0)

# 2. Tạo cột tip_ratio = tip_amount / fare_amount
df_with_ratio = df_filtered.withColumn(
    "tip_ratio", 
    F.round(F.col("tip_amount") / F.col("fare_amount"), 4)
)

# 3. Tìm 20 chuyến có tip_ratio cao nhất
top_20_tip_ratio = df_with_ratio.orderBy(F.desc("tip_ratio")).limit(20)

# Hiển thị kết quả (select thêm vài cột để đối chiếu)
top_20_tip_ratio.select(
    "fare_amount", 
    "tip_amount", 
    "tip_ratio", 
    "PULocationID", 
    "DOLocationID"
).show()

spark.stop()