# reddit-streaming
Để tạo thông tin file credentials.cfg: <br>
  B1: Tạo tài khoản reddit <br>
  B2: vào link: https://www.reddit.com/prefs/apps <br>
  B3: Điền các thông và nhấn create app như hình ![Alt text for the image](image/img1.png) <br>
  B4: Sau B3 kết quả như hình ![Alt text for the image](image/img2.png). Lưu kết quả gồm client_id (dưới dòng personal use script), client_secret (giá trị thuộc tính secret). <br>
  B5: Điền thông tin file credentials.cfg là 2 giá trị vừa lưu ở trên, giá trị user_agent có thể điền tuỳ ý. Lưu ý điền giá trị vào file credentials.cfg không chứa dấu ""; ví dụ : client_id = abcdef <br>
  
# test producer:
1. chạy lệnh docker compose up -d <br>
2. đợi 1 khoảng thời gian, chạy docker ps để kiểm tra status container <br>
3. cài thư viện trong file requirements.txt và chạy file producer.py <br>
4. chạy docker exec -it <container_id_or_name> /bin/bash để vào container broker <br>
5. chạy lệnh: cd /bin và lệnh ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic_name> --from-beginning để check đã có message ở topic chưa <br>

# Spark Streaming:
1. Chạy lệnh ```python3 ./cassandra/create_keyspace_and_table.py``` để tạo Cassandra keyspace và table <br>
2. Nếu chưa chạy file producer: ```cd producer``` -> ```python3 producer.py``` để tạo dữ liệu trong Kafka topic <br>
3. Vào spark-master container: ```docker exec -it spark-master /bin/bash``` <br>
4. Chạy lệnh ```./bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/spark/data/spark_streaming.py``` để submit job lên spark server (<b>Sẽ chạy khá lâu để tải các package cần thiết</b>) <br>
5. Vào Cassandra để check xem dữ liệu đã có chưa: <br>
Mở 1 tab terminal khác: chạy ```docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042``` để vào cassandra container <br>
Chạy ```SELECT * FROM reddit.comments``` để xem đã có dữ liệu chưa
