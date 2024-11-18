from cassandra.cluster import Cluster
import logging

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS reddit
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("DROP TABLE IF EXISTS spark_streams.created_users;")

    # should create table so that can generate primary key
    session.execute("""
        CREATE TABLE IF NOT EXISTS reddit.comments(
            uuid uuid,
            id text,
            name text,
            author text,
            body text,
            subreddit text,
            up_votes int,
            down_votes int,
            over_18 boolean,
            permalink text,
            api_timestamp timestamp,
            ingest_timestamp timestamp,
            PRIMARY KEY((subreddit), api_timestamp)
        )
        WITH CLUSTERING ORDER BY (api_timestamp DESC);
    """)

    session.execute("""
        CREATE INDEX IF NOT EXISTS ON reddit.comments (uuid);
    """)

    print("Table created successfully!")


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


if __name__ == "__main__":
    session = create_cassandra_connection()
    if session is not None:
        create_keyspace(session)
        create_table(session)
