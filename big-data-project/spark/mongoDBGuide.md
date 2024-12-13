# I. Thêm các thư viện vào spark/requirements.txt: 
    pymongo
    python-dotenv

# II. MongoURI:
- Tạo file `.env` trong thư mục spark/spark_app, nội dung:
    `MONGO_URI = mongodb+srv://itksoict:NgaongheIT205@cluster0.6sggc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0`

# III. Chạy chương trình:
- Mở 3 Terminal tới Path .\BigDataProject\big-data-project>
- Terminal 1: Chạy tuần tự các lệnh
    `docker build . -t hust-spark-image`
    `docker-compose up --scale spark-worker=3`
- Terminal 2 và 3 để ở chế độ Split cho dễ quan sát:
    + Terminal 2: (Chạy trước)
        `docker exec hust-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/test.py`
    + Terminal 3: Crawl Data: `python .\data\collect.py -p 10` (10 là số lượng Player)

# IV. Những thứ mới:
- Import thêm thư viện:
    from pymongo import MongoClient
    from dotenv import load_dotenv
- Khai báo các tham số để truy cập MongoDB:
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DATABASE = "BigData"
    MONGO_COLLECTION = "game_results"
- Trong hàm main, biến spark thêm 2 config của MONGO
    spark = (
        SparkSession.builder.appName("KafkaMongoDBStreaming")
        .master("spark://spark-master:7077")
        .config("spark.mongodb.input.uri", MONGO_URI)
        .config("spark.mongodb.output.uri", MONGO_URI)
        .getOrCreate()
    )
- Trong từng batch:
    def process_batch(batch_df, batch_id):
        ...
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]

        for gameResult in gameResults.collect():
            # Convert Spark Row to a dictionary
            document = gameResult.asDict(recursive=True)
            # Insert into MongoDB
            collection.insert_one(document)
            print("Inserted document:", document)
