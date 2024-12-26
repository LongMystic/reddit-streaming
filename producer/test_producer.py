import time
from json import dumps
from kafka import KafkaProducer
import configparser
import praw
import threading
from typing import List, Dict
from datetime import datetime
import random

threads = []
top_subreddit_list = ['AskReddit', 'funny', 'gaming', 'aww', 'worldnews']
countries_subreddit_list = ['VietNam', 'usa', 'unitedkingdom', 'australia', 'russia', 'China']

class RedditProducer:
    def __init__(self, subreddit_list: List[str], message_count: int = 10000, cred_file: str = "../secrets/credentials.cfg"):
        self.subreddit_list = subreddit_list
        self.message_count = message_count
        self.messages_sent = 0
        self.reddit = self.__get_reddit_client__(cred_file)
        self.producer = KafkaProducer(
                bootstrap_servers=['192.168.49.2:32711'],
            value_serializer=lambda x: dumps(x).encode('utf-8'),
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='user1',
            sasl_plain_password='qi8eMBKUW5'
        )
        if self.producer is not None:
            print("Producer is initialized successfully")

    def __get_reddit_client__(self, cred_file) -> praw.Reddit:
        config = configparser.ConfigParser()
        config.read_file(open(cred_file))
        try:
            client_id: str = config.get("reddit", "client_id")
            client_secret: str = config.get("reddit", "client_secret")
            user_agent: str = config.get("reddit", "user_agent")
            print(f"Credentials loaded: {client_id}, {client_secret}, {user_agent}")
        except configparser.NoSectionError:
            raise ValueError("The config file does not contain a reddit credential section.")
        except configparser.NoOptionError as e:
            raise ValueError(f"The config file is missing the option {e}")
        return praw.Reddit(
            user_agent=user_agent,
            client_id=client_id,
            client_secret=client_secret
        )

    def generate_seed_message(self, subreddit_name: str) -> Dict[str, str]:
        return {
            "id": f"id_{random.randint(10000, 99999)}",
            "name": f"t1_{random.randint(100000, 999999)}",
            "author": f"user_{random.randint(1000, 9999)}",
            "body": f"This is a test comment {random.randint(1, 1000)}",
            "subreddit": subreddit_name,
            "upvotes": random.randint(0, 1000),
            "downvotes": random.randint(0, 100),
            "over_18": random.choice([True, False]),
            "timestamp": time.time(),
            "permalink": f"/r/{subreddit_name}/comments/{random.randint(10000, 99999)}/"
        }

    def send_messages(self, subreddit_name: str) -> None:
        messages_per_thread = self.message_count // len(self.subreddit_list)
        
        for _ in range(messages_per_thread):
            try:
                if self.messages_sent >= self.message_count:
                    break

                comment_json = self.generate_seed_message(subreddit_name)
                future = self.producer.send("redditcomments", value=comment_json)
                
                try:
                    record_metadata = future.get(timeout=10)
                    self.messages_sent += 1
                    print(f'Message {self.messages_sent} sent to partition {record_metadata.partition} at offset {record_metadata.offset}')
                except Exception as e:
                    print(f'Failed to send message: {e}')

            except Exception as e:
                print(f"An error occurred: {str(e)}")
                print(f"Error type: {type(e).__name__}")
                import traceback
                print(traceback.format_exc())

    def start_sending_threads(self):
        start_time = time.time()
        
        for subreddit_name in self.subreddit_list:
            thread = threading.Thread(target=self.send_messages, args=(subreddit_name,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\nPerformance Summary:")
        print(f"Total messages sent: {self.messages_sent}")
        print(f"Total time taken: {total_time:.2f} seconds")
        print(f"Average throughput: {self.messages_sent/total_time:.2f} messages/second")

if __name__ == "__main__":
    reddit_producer = RedditProducer(top_subreddit_list + countries_subreddit_list)
    reddit_producer.start_sending_threads()
