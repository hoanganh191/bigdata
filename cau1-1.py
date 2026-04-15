from pyspark.sql import SparkSession

# ================================================================================
# 1. KHỞI TẠO SPARK SESSION
# ================================================================================
spark = SparkSession.builder \
    .appName("NYC_Taxi_Analysis_1.1_Nhom9") \
    .master("local[4]") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "1.5g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================================================================
# 2. ĐỌC TẤT CẢ FILE PARQUET TỪ HDFS
# ================================================================================
# Dấu * giúp Spark đọc tất cả các file có định dạng .parquet trong thư mục
hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/*.parquet"

# Đọc toàn bộ dữ liệu vào một DataFrame duy nhất
df_all = spark.read.parquet(hdfs_path)

# ================================================================================
# 3. HIỂN THỊ KẾT QUẢ THEO YÊU CẦU 1.1
# ================================================================================
print("\n" + "="*70)
print("KẾT QUẢ THỰC HIỆN CÂU 1.1 - NHÓM 9")
print("="*70)

# a. Hiển thị Schema (Cấu trúc dữ liệu)
print("\n[1] HIỂN THỊ SCHEMA (CẤU TRÚC DỮ LIỆU):")
df_all.printSchema()

print("-" * 70)

# b. Tính tổng số chuyến đi (Tổng số dòng của tất cả các tháng)
total_trips = df_all.count()
print(f"\n[2] TỔNG SỐ CHUYẾN ĐI (6 THÁNG ĐẦU NĂM 2023):")
print(f" >>> {total_trips:,} chuyến đi")

print("\n" + "="*70 + "\n")

# Dừng Spark
spark.stop()