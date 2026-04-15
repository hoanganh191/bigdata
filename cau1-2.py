from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ================================================================================
# 1. KHỞI TẠO SPARK - TỐI ƯU CHO MÁY ẢO 8GB
# ================================================================================
spark = SparkSession.builder \
    .appName("NYC_Taxi_Analysis_1.2_Nhom9") \
    .master("local[4]") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "1.5g") \
    .getOrCreate()

# Tắt các thông báo log không cần thiết
spark.sparkContext.setLogLevel("WARN")

# ================================================================================
# 2. ĐỌC DỮ LIỆU TỪ HDFS
# ================================================================================
# Sử dụng dấu * để đọc toàn bộ 6 file parquet trong thư mục
hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/*.parquet"
df_raw = spark.read.parquet(hdfs_path)

# ================================================================================
# 3. TIỀN XỬ LÝ: LÀM SẠCH DỮ LIỆU (LOẠI BỎ DATA NOISE)
# ================================================================================
# Lọc bỏ các tháng lạ (nhiễu thiết bị), chỉ giữ lại tháng 1-6 năm 2023
df_cleaned = df_raw.filter(
    (F.year("tpep_pickup_datetime") == 2023) & 
    (F.month("tpep_pickup_datetime") <= 6)
).withColumn("Month", F.month("tpep_pickup_datetime"))

# ================================================================================
# 4. THỰC HIỆN CÂU 1.2: TỔNG SỐ CHUYẾN THEO THÁNG - SẮP XẾP GIẢM DẦN
# ================================================================================
print("\n" + "="*80)
print("KẾT QUẢ CÂU 1.2: THỐNG KÊ TỔNG SỐ CHUYẾN THEO THÁNG (GIẢM DẦN)")
print("="*80)

# Gom nhóm theo tháng và đếm số lượng bản ghi
monthly_counts = df_cleaned.groupBy("Month") \
    .count() \
    .withColumnRenamed("count", "Total_Trips") \
    .orderBy(F.desc("Total_Trips"))

# Hiển thị kết quả ra màn hình
monthly_counts.show()


# Dừng Spark Session
spark.stop()