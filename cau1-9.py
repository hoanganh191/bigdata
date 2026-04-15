from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.9_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

def read_standard_1_9(f):
    return spark.read.parquet(hdfs_path + f).select(
        F.col("trip_distance").cast(DoubleType())
    )

# Gộp dữ liệu 6 tháng
df_all = reduce(lambda df1, df2: df1.union(df2), [read_standard_1_9(f) for f in files])

print("\n" + "="*65)
print("KẾT QUẢ CÂU 1.9: PHÂN LOẠI CHUYẾN ĐI THEO KHOẢNG CÁCH (NHÓM 9)")
print("="*65)

# 1. Tạo cột trip_distance_level dựa trên điều kiện của đề bài
df_leveled = df_all.withColumn(
    "trip_distance_level",
    F.when(F.col("trip_distance") < 2, "short")
     .when((F.col("trip_distance") >= 2) & (F.col("trip_distance") < 10), "medium")
     .otherwise("long")
)

# 2. Thống kê số lượng chuyến đi ở mỗi mức độ để kiểm tra kết quả
distance_stats = df_leveled.groupBy("trip_distance_level").count()

# 3. Hiển thị mẫu dữ liệu (10 dòng đầu) kèm cột mới
print("Mẫu dữ liệu sau khi thêm cột phân loại:")
df_leveled.select("trip_distance", "trip_distance_level").show(10)

print("Thống kê số lượng theo từng cấp độ:")
distance_stats.show()

spark.stop()