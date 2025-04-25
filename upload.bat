@echo off
setlocal enabledelayedexpansion

REM Cấu hình đường dẫn local
set DATADIR=E:\PythonProject\bigdata\minh\data

echo 🔄 Copy các file CSV từ local vào container hadoop-namenode...

for %%f in (%DATADIR%\2024_*.csv) do (
    echo   → Copy %%~nxf vào container...
    docker cp "%%f" hadoop-namenode:/tmp/%%~nxf
)

echo ✅ Copy xong!

echo 🔼 Đẩy dữ liệu vào HDFS...
for %%f in (%DATADIR%\2024_*.csv) do (
    docker exec hadoop-namenode hdfs dfs -mkdir -p data
    docker exec hadoop-namenode hdfs dfs -put -f /tmp/%%~nxf /data/
)

echo ✅ Đã đẩy dữ liệu vào HDFS!