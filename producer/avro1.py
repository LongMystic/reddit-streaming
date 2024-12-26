import time
import random
import asyncio
import string
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from typing import Dict

class KafkaLoadTest:
   def __init__(self, num_messages=10000):
       self.num_messages = num_messages
       self.success_count = 0
       self.error_count = 0

       # Define Avro schema
       value_schema_str = """
       {
           "type": "record",
           "namespace": "com.reddit",
           "name": "Comment",
           "fields": [
               {"name": "id", "type": "string"},
               {"name": "name", "type": "string"},
               {"name": "author", "type": "string"},
               {"name": "body", "type": "string"},
               {"name": "subreddit", "type": "string"},
               {"name": "up_votes", "type": "int"},
               {"name": "down_votes", "type": "int"}, 
               {"name": "over_18", "type": "boolean"},
               {"name": "timestamp", "type": "double"},
               {"name": "permalink", "type": "string"}
           ]
       }
       """
       value_schema = avro.loads(value_schema_str)

       try:
           self.producer = AvroProducer({
               'bootstrap.servers': 'kafka:9092',
               'schema.registry.url': 'http://schema-registry:8081'
           }, default_value_schema=value_schema)
           print("Producer initialized successfully")
       except Exception as e:
           print(f"Error initializing producer: {str(e)}")
           raise

   def generate_random_text(self, length=100):
       """Generate random text of given length"""
       letters = string.ascii_letters + ' ' * 10
       return ''.join(random.choice(letters) for _ in range(length))

   def generate_message(self) -> Dict[str, any]:
       comment_id = f"t1_{random.randint(100000000, 999999999)}"
       
       return {
           "id": comment_id,
           "name": f"t1_{comment_id}",
           "author": f"user_{random.randint(1, 10000)}",
           "body": self.generate_random_text(200),
           "subreddit": random.choice(['AskReddit', 'funny', 'gaming', 'aww', 'worldnews', 
                                     'VietNam', 'usa', 'unitedkingdom', 'australia', 'russia', 'China']),
           "up_votes": random.randint(1, 10000),
           "down_votes": random.randint(0, 100),
           "over_18": random.choice([True, False]),
           "timestamp": time.time(),
           "permalink": f"/r/subreddit/comments/{comment_id}"
       }

   async def run_load_test(self):
       print(f"Starting load test - sending {self.num_messages} messages...")
       start_time = time.time()

       try:
           for i in range(self.num_messages):
               message = self.generate_message()
               
               # Produce message với Avro serialization
               self.producer.produce(
                   topic="reddit-comments",
                   value=message
               )
               self.success_count += 1

               # Poll mỗi 1000 messages
               if self.success_count % 1000 == 0:
                   print(f"Sent {self.success_count} messages")
                   self.producer.poll(0)

       except Exception as e:
           print(f"Error in test: {str(e)}")
           raise

       finally:
           # Flush cuối cùng để đảm bảo tất cả messages đã được gửi
           self.producer.flush()
           
           end_time = time.time()
           duration = end_time - start_time

           print("\nLoad Test Results:")
           print("-" * 50)
           print(f"Messages:")
           print(f"  Total sent: {self.success_count + self.error_count}")
           print(f"  Successful: {self.success_count}")
           print(f"  Failed: {self.error_count}")
           print(f"\nPerformance:")
           print(f"  Duration: {duration:.2f} seconds")
           print(f"  Throughput: {self.success_count/duration:.2f} msgs/sec")
           print("-" * 50)

async def main():
   test = KafkaLoadTest(num_messages=10000)
   await test.run_load_test()

if __name__ == "__main__":
   asyncio.run(main())
