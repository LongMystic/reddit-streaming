from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from typing import Dict, Any
import json
import time
import random
from datetime import datetime, timedelta
import faker

# Định nghĩa Avro schema cho Reddit comments
value_schema_str = """
{
    "namespace": "reddit.avro",
    "type": "record",
    "name": "RedditComment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "author", "type": "string"},
        {"name": "body", "type": "string"},
        {"name": "subreddit", "type": "string"},
        {"name": "upvotes", "type": "int"},
        {"name": "downvotes", "type": "int"},
        {"name": "over_18", "type": "boolean"},
        {"name": "timestamp", "type": "double"},
        {"name": "permalink", "type": "string"}
    ]
}
"""

def create_producer() -> AvroProducer:
    config = {
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081'
    }
    
    value_schema = avro.loads(value_schema_str)
    
    return AvroProducer(
        config,
        default_value_schema=value_schema
    )

class RedditKafkaProducer:
    def __init__(self):
        self.producer = create_producer()
        self.topic = "reddit-comments"
        self.fake = faker.Faker()
    
    def generate_fake_comment(self) -> Dict[str, Any]:
        """Tạo dữ liệu comment giả"""
        comment_id = self.fake.uuid4()
        timestamp = time.time() - random.randint(0, 86400)  # Random time trong 24h qua
        
        return {
            "id": comment_id,
            "name": f"t1_{comment_id}",
            "author": self.fake.user_name(),
            "body": self.fake.text(max_nb_chars=200),
            "subreddit": random.choice(["python", "programming", "technology", "news", "games"]),
            "upvotes": random.randint(1, 1000),
            "downvotes": random.randint(0, 100),
            "over_18": random.choice([True, False]),
            "timestamp": timestamp,
            "permalink": f"/r/subreddit/comments/{comment_id}"
        }

    def send_message(self, data: Dict[str, Any]) -> None:
        try:
            self.producer.produce(
                topic=self.topic,
                value=data
            )
        except Exception as e:
            print(f"Lỗi khi gửi message: {str(e)}")
    
    def send_batch_comments(self, num_comments: int) -> None:
        """Gửi một loạt comments và đo thời gian"""
        start_time = time.time()
        messages_sent = 0
        
        print(f"Bắt đầu gửi {num_comments} comments...")
        
        for i in range(num_comments):
            comment_data = self.generate_fake_comment()
            self.send_message(comment_data)
            messages_sent += 1
            
            # Flush sau mỗi 100 messages để tránh memory pressure
            if i > 0 and i % 100 == 0:
                self.producer.flush()
                print(f"Đã gửi {messages_sent} messages...")
        
        # Final flush
        self.producer.flush()
        
        end_time = time.time()
        total_time = end_time - start_time
        rate = num_comments / total_time
        
        print(f"\nKết quả:")
        print(f"Tổng số messages đã gửi: {messages_sent}")
        print(f"Tổng thời gian: {total_time:.2f} giây")
        print(f"Tốc độ trung bình: {rate:.2f} messages/giây")
    
    def close(self):
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    # Khởi tạo producer
    kafka_producer = RedditKafkaProducer()
    
    try:
        # Gửi 1000 comments
        kafka_producer.send_batch_comments(1000)
    finally:
        kafka_producer.close()
