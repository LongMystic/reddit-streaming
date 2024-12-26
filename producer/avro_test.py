import time
import random
import asyncio
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

class KafkaLoadTest:
   def __init__(self, num_messages=10000):
       self.num_messages = num_messages
       self.success_count = 0
       self.error_count = 0
       self.total_bytes = 0

       # Define Avro schema
       value_schema_str = """
       {
           "type": "record",
           "namespace": "com.test",
           "name": "Message",
           "fields": [
               {"name": "id", "type": "string"},
               {"name": "timestamp", "type": "double"},
               {"name": "data", "type": "string"}
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

   def generate_message(self, size_bytes=1000):
       return {
           "id": f"msg_{random.randint(1, 1000000)}",
           "timestamp": time.time(),
           "data": "x" * size_bytes
       }

   async def run_load_test(self):
       print(f"Starting load test - sending {self.num_messages} messages...")
       start_time = time.time()

       try:
           for i in range(self.num_messages):
               message = self.generate_message()
               
               # Với AvroProducer, sử dụng produce thay vì send
               self.producer.produce(
                   topic="test-topic",
                   value=message
               )
               self.success_count += 1

               if self.success_count % 1000 == 0:
                   print(f"Sent {self.success_count} messages")
                   # Poll để đảm bảo callback được xử lý
                   self.producer.poll(0)

       except Exception as e:
           print(f"Error in test: {str(e)}")
           raise

       finally:
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
