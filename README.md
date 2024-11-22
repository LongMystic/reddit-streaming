# reddit-streaming
## Tạo thông tin file credentials.cfg:
1. Tạo tài khoản reddit
2. Vào link: https://www.reddit.com/prefs/apps
3. Điền các thông và nhấn create app như hình ![Alt text for the image](image/img1.png)
4. Sau Bước 3 kết quả như hình ![Alt text for the image](image/img2.png). Lưu kết quả gồm client_id (dưới dòng personal use script), client_secret (giá trị thuộc tính secret)
5. Điền thông tin file credentials.cfg là 2 giá trị vừa lưu ở trên, giá trị user_agent có thể điền tuỳ ý. Lưu ý điền giá trị vào file credentials.cfg không chứa dấu ""; ví dụ : client_id = abcdef
  
## Test producer:
1. Chạy lệnh `docker compose up -d`
2. Đợi 1 khoảng thời gian, chạy `docker ps` để kiểm tra status container
3. Cài các thư viện Python cần thiết `pip install -r requirements.txt` và chạy file producer.py
4. Chạy `docker exec -it <container_id_or_name> /bin/bash` để vào container broker
5. Chạy lệnh: `cd /bin` và lệnh `./kafka-console-consumer --bootstrap-server localhost:9092 --topic redditcomments --from-beginning` để check đã có message ở topic chưa

## Spark Streaming:
1. Chạy lệnh ```python3 ./cassandra/create_keyspace_and_table.py``` để tạo Cassandra keyspace và table
2. Nếu chưa chạy file producer: ```cd producer``` -> ```python3 producer.py``` để tạo dữ liệu trong Kafka topic
3. Vào spark-master container: ```docker exec -it spark-master /bin/bash```
4. Chạy lệnh ```./bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/spark/data/spark_streaming.py``` để submit job lên spark server (<b>Sẽ chạy khá lâu để tải các package cần thiết</b>)
5. Vào Cassandra để check xem dữ liệu đã có chưa:  
Mở 1 tab terminal khác: chạy ```docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042``` để vào cassandra container
Chạy ```SELECT * FROM reddit.comments``` để xem đã có dữ liệu chưa
