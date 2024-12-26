import time
from json import dumps
from kafka import KafkaProducer
import random
from typing import List, Dict
from concurrent.futures import Future

class KafkaPerformanceTest:
    def __init__(self, message_count: int = 1000):
        self.message_count = message_count
        self.single_node_producer = KafkaProducer(
            bootstrap_servers=['192.168.49.2:31346'],
            value_serializer=lambda x: dumps(x).encode('utf-8'),
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='user1',
            sasl_plain_password='YVPS6F2dia',
            batch_size=16384,  # Increased batch size
            linger_ms=50,      # Wait up to 50ms for batching
        )
        
        self.multi_node_producer = KafkaProducer(
            bootstrap_servers=['192.168.49.2:31346', '192.168.49.2:31209', '192.168.49.2:32418'],
            value_serializer=lambda x: dumps(x).encode('utf-8'),
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='user1',
            sasl_plain_password='YVPS6F2dia',
            batch_size=16384,
            linger_ms=50,
        )

    def generate_message(self) -> Dict[str, any]:
        return {
            "id": f"id_{random.randint(10000, 99999)}",
            "name": f"t1_{random.randint(100000, 999999)}",
            "author": f"user_{random.randint(1000, 9999)}",
            "body": f"This is a test comment {random.randint(1, 1000)}",
            "subreddit": "test_subreddit",
            "upvotes": random.randint(0, 1000),
            "downvotes": random.randint(0, 100),
            "over_18": random.choice([True, False]),
            "timestamp": time.time(),
            "permalink": f"/r/test/comments/{random.randint(10000, 99999)}/"
        }

    def test_single_node(self) -> Dict[str, float]:
        print("\nTesting Single Node Performance...")
        start_time = time.time()
        futures: List[Future] = []
        
        # Gửi tất cả messages mà không chờ đợi
        for i in range(self.message_count):
            message = self.generate_message()
            future = self.single_node_producer.send("redditcomments", value=message)
            futures.append(future)
            if i % 100 == 0:
                print(f"Queued {i} messages...")
        
        # Flush và đợi tất cả messages được gửi
        print("Flushing and waiting for all messages...")
        self.single_node_producer.flush()
        
        # Kiểm tra kết quả
        successful = 0
        errors = 0
        for future in futures:
            try:
                future.get(timeout=10)
                successful += 1
            except Exception as e:
                errors += 1
                print(f"Error: {str(e)}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        return {
            "total_time": total_time,
            "messages_sent": successful,
            "errors": errors,
            "throughput": successful/total_time if total_time > 0 else 0
        }

    def test_multi_node(self) -> Dict[str, float]:
        print("\nTesting Multi-Node Performance...")
        start_time = time.time()
        futures: List[Future] = []
        
        # Gửi tất cả messages mà không chờ đợi
        for i in range(self.message_count):
            message = self.generate_message()
            future = self.multi_node_producer.send("redditcomments", value=message)
            futures.append(future)
            if i % 100 == 0:
                print(f"Queued {i} messages...")
        
        # Flush và đợi tất cả messages được gửi
        print("Flushing and waiting for all messages...")
        self.multi_node_producer.flush()
        
        # Kiểm tra kết quả
        successful = 0
        errors = 0
        for future in futures:
            try:
                future.get(timeout=10)
                successful += 1
            except Exception as e:
                errors += 1
                print(f"Error: {str(e)}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        return {
            "total_time": total_time,
            "messages_sent": successful,
            "errors": errors,
            "throughput": successful/total_time if total_time > 0 else 0
        }

    def run_comparison(self, iterations: int = 3):
        print(f"Starting performance comparison with {self.message_count} messages per test")
        print(f"Running {iterations} iterations for each configuration")
        
        single_node_results = []
        multi_node_results = []
        
        for i in range(iterations):
            print(f"\nIteration {i+1}/{iterations}")
            
            # Test single node
            single_result = self.test_single_node()
            single_node_results.append(single_result)
            
            # Small delay between tests
            time.sleep(2)
            
            # Test multi node
            multi_result = self.test_multi_node()
            multi_node_results.append(multi_result)
            
            # Small delay between iterations
            time.sleep(2)
        
        # Calculate averages
        def calculate_averages(results):
            total_time_avg = sum(r["total_time"] for r in results) / len(results)
            throughput_avg = sum(r["throughput"] for r in results) / len(results)
            errors_avg = sum(r["errors"] for r in results) / len(results)
            return {
                "avg_time": total_time_avg,
                "avg_throughput": throughput_avg,
                "avg_errors": errors_avg
            }
        
        single_node_avg = calculate_averages(single_node_results)
        multi_node_avg = calculate_averages(multi_node_results)
        
        # Print detailed results
        print("\n=== Performance Comparison Results ===")
        print("\nSingle Node Average Results:")
        print(f"Average Time: {single_node_avg['avg_time']:.2f} seconds")
        print(f"Average Throughput: {single_node_avg['avg_throughput']:.2f} messages/second")
        print(f"Average Errors: {single_node_avg['avg_errors']:.2f}")
        
        print("\nMulti-Node Average Results:")
        print(f"Average Time: {multi_node_avg['avg_time']:.2f} seconds")
        print(f"Average Throughput: {multi_node_avg['avg_throughput']:.2f} messages/second")
        print(f"Average Errors: {multi_node_avg['avg_errors']:.2f}")
        
        # Calculate improvement
        throughput_improvement = ((multi_node_avg['avg_throughput'] - single_node_avg['avg_throughput']) 
                                / single_node_avg['avg_throughput'] * 100)
        print(f"\nThroughput Improvement: {throughput_improvement:.2f}%")

if __name__ == "__main__":
    tester = KafkaPerformanceTest(message_count=10000)
    tester.run_comparison(iterations=3)
