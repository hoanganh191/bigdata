# Giải thích bài toán và cách làm `cau2`

## 1. Mục tiêu đề bài

Câu 2 yêu cầu dùng chuỗi file Parquet theo tháng để thực hiện streaming Top 5 vùng đón khách có số chuyến tăng mạnh nhất theo ngày. Kết quả cần hiển thị các cột:
- `PULocationID`
- `trip_date`
- `trip_count`
- `growth_rate`

## 2. Quy trình chính

Bài toán được chia thành 2 bước chính:

### Bước 1: Chuẩn hóa dữ liệu

File `cau2_prepare.py` thực hiện:
- đọc các file Parquet gốc từ HDFS đường dẫn `/taxi/`
- giữ lại chỉ 2 cột cần dùng là `PULocationID` và `tpep_pickup_datetime`
- đổi kiểu dữ liệu về đúng loại (`long` cho `PULocationID`, `timestamp` cho `tpep_pickup_datetime`)
- ghi dữ liệu chuẩn hóa vào thư mục mới `/data_taxi_normalized/`
- mỗi file gốc sẽ thành một thư mục con, ví dụ:
  - `/data_taxi_normalized/03_yellow_tripdata_2023-01.parquet/`
  - `/data_taxi_normalized/03_yellow_tripdata_2023-02.parquet/`

### Bước 2: Streaming xử lý Top 5 vùng tăng mạnh nhất

File `cau2_streaming.py` thực hiện:
- đọc dữ liệu parquet dạng streaming từ đường dẫn wildcard:
  - `hdfs://localhost:9000/data_taxi_normalized/03_yellow_tripdata_2023-*/`
- sử dụng `Spark Structured Streaming`
- đặt sẵn schema cho nguồn streaming gồm 2 cột cần thiết
- chuyển `tpep_pickup_datetime` thành `trip_date` chỉ lấy ngày
- giữ dữ liệu trong năm 2023
- nhóm theo `trip_date` và `PULocationID` để tính `trip_count`
- tính `growth_rate` so với ngày trước đó cho mỗi `PULocationID`
- chọn Top 5 mỗi ngày theo `growth_rate`
- in kết quả ra console

## 3. Ý nghĩa từng cột trong kết quả

Bảng kết quả cuối cùng hiển thị các cột sau:

- `PULocationID`
  - là mã vùng pickup location, lấy trực tiếp từ cột `PULocationID` trong dữ liệu gốc.
  - mỗi mã đại diện cho một khu vực ở NYC.
- `trip_date`
  - là ngày lấy khách, lấy từ cột `tpep_pickup_datetime` bằng hàm `to_date()`.
  - chỉ giữ phần ngày, bỏ giờ/phút/giây.
- `trip_count`
  - là số chuyến taxi được đếm trong cùng một ngày `trip_date` cho một `PULocationID` cụ thể.
  - được tính bằng `count(*)` sau khi nhóm theo `trip_date` và `PULocationID`.
- `growth_rate`
  - là tỷ lệ tăng/giảm số chuyến so với ngày trước đó của cùng một `PULocationID`.
  - nếu ngày trước đó có `trip_count` thì dùng công thức:
    - `(trip_count - prev_count) / prev_count * 100`
  - nếu không có ngày trước hoặc `prev_count = 0` thì đặt `growth_rate = 0`.

## 4. Giải thích chi tiết phần `growth_rate`

Ở `cau2_streaming.py`, sau khi đã có `daily_counts`:
- ta dùng window `Window.partitionBy("PULocationID").orderBy("trip_date")`
- với mỗi `PULocationID`, ta lấy giá trị `trip_count` của ngày trước đó (`prev_count`)
- tính `growth_rate` bằng công thức:
  - `(trip_count - prev_count) / prev_count * 100`
- nếu ngày trước không tồn tại hoặc `prev_count = 0` thì đặt `growth_rate = 0`

Vì đề bài yêu cầu top 5 vùng có chuyến tăng mạnh nhất theo ngày, nên ta sắp xếp giảm dần theo `growth_rate` mỗi `trip_date`.

## 4. Các điểm cần lưu ý

- `outputMode("complete")` được dùng trong streaming để có đủ dữ liệu cho toàn bộ ngày mỗi lần tính toán Top 5.
- `checkpointLocation` cần thiết để Spark lưu trạng thái streaming và tránh bị lỗi khi restart.
- đường dẫn wildcard `03_yellow_tripdata_2023-*/` cho phép đọc tất cả các thư mục tháng của năm 2023.

## 5. Cách chạy

Chạy toàn bộ pipeline bằng:
```bash
spark-submit cau2.py
```

Hoặc chạy riêng mỗi bước:
```bash
spark-submit cau2_prepare.py
spark-submit cau2_streaming.py
```

## 6. Kết luận

Code mà bạn đang làm đã đúng định hướng đề bài: chuẩn hóa dữ liệu trước, sau đó chạy streaming để tính `trip_count` và `growth_rate`, rồi chọn Top 5 mỗi ngày.

Nếu bạn muốn nộp, chỉ cần giải thích rõ 2 bước này và nhấn mạnh rằng `growth_rate` được tính theo ngày trước đó cho từng `PULocationID`.
