# Hướng dẫn sử dụng:

## I/ Cài đặt:

- Đảm bảo máy có python, pip, docker compose.
- Không có thì tải

## II/ Sử dụng

1. Cài đặt kafka:
- Vào trong  `/analysis` , chạy `docker-compose up`
- kafka broker sẽ được dựng tai `localhost:9092`

2. Cài đặt các thư viện trong python, cài đặt tài khoản:
- Vào trong `/data`, chạy:
```
pip install -r requirement.txt
```
- Tạo 1 tài khoản trên: `https://developer.clashroyale.com/#/account`
- Sau đó, cho email và mật khẩu tài khoản trê của mình vào trong file `.env`, tạo trong thư mục `/data`
- VD: 
```
API_CLASH_ROYALE_PASSWORD=password
API_CLASH_ROYALE_EMAIL=email@gmail.com
```
3. Tạo topic trong kafka.
- Vào trong mục `/kafka`, chạy 
```
python topic_create.py
```
- Để tạo 1 topic tên `big_data_topic` trong broker.
- Kiểm tra thông qua `topic_list.py`
- Tạo 1 consumer ảo thông qua `topic_create.py`

4. Crawl data:
- Vào thư mục `/data`, chạy:
```
python collect.py -p [số player]
```
- Sau đó, dữ liệu sẽ được gửi vào topic thành công.