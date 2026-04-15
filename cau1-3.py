from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ================================================================================
# 1. KHỞI TẠO SPARK - TỐI ƯU CHO MÁY ẢO 8GB
# ================================================================================
spark = SparkSession.builder \
    .appName("NYC_Taxi_Analysis_1.3_Nhom9") \
    .master("local[4]") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "1.5g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================================================================
# 2. ĐỌC DỮ LIỆU TỪ HDFS
# ================================================================================
# Sử dụng dấu * để nạp toàn bộ dữ liệu 6 tháng
hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/*.parquet"
df_raw = spark.read.parquet(hdfs_path)

# ================================================================================
# 3. TIỀN XỬ LÝ: LÀM SẠCH DỮ LIỆU (QUAN TRỌNG CHO TÍNH TRUNG BÌNH)
# ================================================================================
# - Lọc đúng năm 2023 và tháng 1-6
# - Tạo cột Month để gom nhóm
df_cleaned = df_raw.filter(
    (F.year("tpep_pickup_datetime") == 2023) & 
    (F.month("tpep_pickup_datetime") <= 6)
).withColumn("Month", F.month("tpep_pickup_datetime"))

# ================================================================================
# 4. THỰC HIỆN CÂU 1.3: TÍNH QUÃNG ĐƯỜNG & TIỀN VÉ TRUNG BÌNH THEO THÁNG
# ================================================================================
print("\n" + "="*85)
print("KẾT QUẢ CÂU 1.3: QUÃNG ĐƯỜNG TRUNG BÌNH VÀ TIỀN VÉ TRUNG BÌNH THEO TỪNG THÁNG")
print("="*85)

# Sử dụng .agg để tính nhiều giá trị trung bình cùng lúc
# Sử dụng .round để làm tròn 2 chữ số thập phân cho báo cáo chuyên nghiệp
avg_analysis = df_cleaned.groupBy("Month") \
    .agg(
        F.round(F.avg("trip_distance"), 2).alias("Avg_Distance (Dặm)"),
        F.round(F.avg("fare_amount"), 2).alias("Avg_Fare (USD)")
    ) \
    .orderBy("Month")

# Hiển thị kết quả
avg_analysis.show()

print("="*85 + "\n")

# Dừng Spark
spark.stop()