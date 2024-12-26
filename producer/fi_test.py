import time
from json import dumps
from kafka import KafkaProducer
import random
import asyncio
from typing import Dict
import string

class KafkaLoadTest:
   def __init__(self, num_messages=10000):
       self.num_messages = num_messages
       self.success_count = 0
       self.error_count = 0
       self.total_bytes = 0
       try:
           self.producer = KafkaProducer(
               bootstrap_servers=['kafka:9092'],
               value_serializer=lambda x: dumps(x).encode('utf-8'),
               acks=1,
               batch_size=16384,
               linger_ms=0,
           )
           print("Producer initialized successfully")
       except Exception as e:
           print(f"Error initializing producer: {str(e)}")
           raise

   def generate_random_text(self, length=100):
       """Generate random text of given length"""
       letters = string.ascii_letters + ' ' * 10  # Add extra spaces for readability
       return ''.join(random.choice(letters) for _ in range(length))

   def generate_message(self) -> Dict[str, any]:
       comment_id = f"t1_{random.randint(100000000, 999999999)}"
       
       return {
           "id": comment_id,
           "name": f"t1_{comment_id}",
           "author": f"user_{random.randint(1, 10000)}",
           "body": self.generate_random_text(200),  # Random text of 200 chars
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
               message_bytes = dumps(message).encode('utf-8')
               self.total_bytes += len(message_bytes)
               self.producer.send("redditcomments", value=message)
               self.success_count += 1
               if self.success_count % 1000 == 0:
                   print(f"Sent {self.success_count} messages")
           print("All messages sent, flushing producer...")
           self.producer.flush()
       except Exception as e:
           print(f"Error in test: {str(e)}")
           raise
       finally:
           end_time = time.time()
           duration = end_time - start_time
           mb_sent = self.total_bytes / (1024 * 1024)
           print("\nLoad Test Results:")
           print("-" * 50)
           print(f"Messages:")
           print(f"  Total sent: {self.success_count + self.error_count}")
           print(f"  Successful: {self.success_count}")
           print(f"  Failed: {self.error_count}")
           print(f"\nPerformance:")
           print(f"  Duration: {duration:.2f} seconds")
           print(f"  Throughput: {self.success_count/duration:.2f} msgs/sec")
           print(f"  Data rate: {mb_sent/duration:.2f} MB/sec")
           print("-" * 50)

async def main():
   test = KafkaLoadTest(num_messages=10000)
   await test.run_load_test()

if __name__ == "__main__":
   asyncio.run(main())
