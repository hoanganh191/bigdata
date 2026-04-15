from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from functools import reduce

# Khởi tạo Spark
spark = SparkSession.builder.appName("NYC_Taxi_1.6_Nhom9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://localhost:9000/user/nhom9/data_taxi/"
files = [f"03_yellow_tripdata_2023-{str(i).zfill(2)}.parquet" for i in range(1, 7)]

def read_standard_1_6(f):
    # Lấy thêm cột tháng để thống kê chi tiết nếu cần
    return spark.read.parquet(hdfs_path + f).select(
        F.col("trip_distance").cast(DoubleType()),
        F.col("fare_amount").cast(DoubleType())
    )

# Gộp dữ liệu 6 tháng
df_all = reduce(lambda df1, df2: df1.union(df2), [read_standard_1_6(f) for f in files])

print("\n" + "="*60)
print("KẾT QUẢ CÂU 1.6: THỐNG KÊ DỮ LIỆU BẤT THƯỜNG (NHÓM 9)")
print("="*60)

# 1. Tính số chuyến có trip_distance = 0
zero_distance_count = df_all.filter(F.col("trip_distance") == 0).count()

# 2. Tính số chuyến có fare_amount <= 0
invalid_fare_count = df_all.filter(F.col("fare_amount") <= 0).count()

# Tổng số chuyến để tính tỷ lệ % (tùy chọn để báo cáo hay hơn)
total_trips = df_all.count()

print(f"- Số chuyến có quãng đường bằng 0 (trip_distance = 0): {zero_distance_count:,} chuyến")
print(f"- Số chuyến có giá vé nhỏ hơn hoặc bằng 0 (fare_amount <= 0): {invalid_fare_count:,} chuyến")
print(f"- Tổng số chuyến đã kiểm tra: {total_trips:,} chuyến")
print("="*60)

spark.stop()

# Giải thích chi tiết cho báo cáo:

# 1. Hiện tượng quãng đường bằng 0 (trip_distance = 0):

#     Về kỹ thuật: Đây là các bản ghi mà tọa độ điểm đón và điểm trả khách trùng nhau hoặc thiết bị đo quãng đường (odometer) không hoạt động.

#     Về nghiệp vụ: Có thể do tài xế vô tình kích hoạt chuyến đi trên ứng dụng khi chưa thực sự di chuyển, hoặc khách hàng lên xe rồi đổi ý xuống ngay (Cancelled trips).

# 2. Hiện tượng giá vé bất thường (fare_amount <= 0):

#     Giá trị âm (< 0): Trong chuẩn dữ liệu của NYC Taxi, các giá trị âm thường đại diện cho lệnh Dispute (tranh chấp) hoặc Refund (hoàn tiền). Khi một giao dịch bị lỗi, hệ thống tạo ra một bản ghi đối ứng có giá trị âm để triệt tiêu số tiền cũ.

#     Giá trị bằng 0 (= 0): Có thể là các chuyến đi nội bộ, chuyến đi miễn phí theo chương trình khuyến mãi hoặc lỗi trong quá trình đồng bộ hóa dữ liệu từ máy cà thẻ về server trung tâm.