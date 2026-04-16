#!/usr/bin/env python3
"""
Streaming Top 5 vùng đón khách có số chuyến tăng mạnh nhất theo ngày
Gồm 2 bước:
1. Chuẩn hóa dữ liệu (cau2_prepare.py)
2. Streaming từ dữ liệu chuẩn hóa (cau2_streaming.py)
"""
import subprocess
import sys

print("="*80)
print("NYC TAXI STREAMING - TOP 5 VÙNG CÓ SỐ CHUYẾN TĂNG MẠNH NHẤT")
print("="*80)
print("\n[CHUẨN HÓA] /taxi/ → /data_taxi_normalized/")
print("[STREAMING] /data_taxi_normalized/03_yellow_tripdata_2023-*/")

# Bước 1: Chuẩn hóa dữ liệu
print("\n[BƯỚC 1/2] Chuẩn hóa dữ liệu...")
sys.stdout.flush()

prepare_result = subprocess.run(
    ["spark-submit", "cau2_prepare.py"],
    cwd="/home/hoanganh/taxi_project"
)

if prepare_result.returncode != 0:
    print("[ERROR] Chuẩn hóa dữ liệu thất bại!")
    print("Hãy đảm bảo HDFS đang chạy và có file parquet trong /taxi/")
    sys.exit(1)

print("\n✓ Chuẩn hóa xong!\n")

# Bước 2: Streaming
print("\n[BƯỚC 2/2] Bắt đầu streaming...")
sys.stdout.flush()

streaming_result = subprocess.run(
    ["spark-submit", "cau2_streaming.py"],
    cwd="/home/hoanganh/taxi_project"
)

print("\n" + "="*80)
if streaming_result.returncode == 0 or streaming_result.returncode == 130:
    print("✓ Hoàn thành!")
else:
    print("⚠ Streaming gặp lỗi!")
print("="*80)
