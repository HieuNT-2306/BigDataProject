# I. Cài đặt PySpark: 
- Chạy bằng Terminal:
    `pip install pyspark`
- Kiểm tra cài đặt thành công:
    `python -c "import pyspark; print(pyspark.__version__)"`

# II. Cài đặt Java 1.8 (bắt buộc, nếu chưa có):
- Cài đặt tại:
    `https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html`
- Thêm biến môi trường:
    + Vào `This PC -> Properties -> Advance System Settings -> Environment Variables... `
    + Ở cả 2 phần `User variables` và `System variables` : 
        * Chọn New...
            Variable name : `JAVA_HOME`
            Variable value : Browse Directory tới folder vừa cài đặt JDK, ví dụ: `D:\Applications\Java\jdk1.8.0_202`
        * Chọn Variable PATH -> New -> để giá trị : `%JAVA_HOME%\bin`
- Kiểm tra lại bằng lệnh trong Terminal : `java -version`

# III. Tải Apache Spark:
- Tải về, giải nén ra: `https://www.apache.org/dyn/closer.lua/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz`
- Tải Source này về: `https://github.com/s911415/apache-hadoop-3.1.0-winutils`
    + Vào thư mục `bin`, copy toàn bộ vào thư mục `spark-3.5.3-bin-hadoop3\bin`. Bỏ qua những file đã tồn tại
- Thêm các Variables tương tự phía trên:
    + System Variables:
        `PYSPARK_PYTHON` : Path đến file python, VD `D:\Applications\python\Python313\python.exe`
        `PYSPARK_DRIVER_PYTHON` : Path đến file python, VD `D:\Applications\python\Python313\python.exe` 
        `SPARK_HOME` : Path đến thư mục Spark, VD `D:\Applications\spark-3.5.3-bin-hadoop3`
        Sửa `PATH`: Thêm 2 variable: `%SPARK_HOME%\bin` và `%SPARK_HOME%\sbin`
- Kiểm tra cài đặt thành công : `spark-shell --version`

# IV. Chạy chương trình:
Mở Terminal:
    `git clone https://github.com/HieuNT-2306/BigDataProject/`
    `cd BigDataProject/big-data-project/pyspark`
    `python kafka_to_spark.py`

