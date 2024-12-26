from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType, FloatType
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_sentiment(text):
    """
    Analyze sentiment of text with error handling
    """
    try:
        if text is None:
            return 0.0
        analyzer = SentimentIntensityAnalyzer()
        sentiment = analyzer.polarity_scores(text)
        return sentiment['compound']
    except Exception as e:
        logger.error(f"Error in sentiment analysis: {str(e)}")
        return 0.0

# Create UDF for sentiment analysis
sentiment_udf = F.udf(analyze_sentiment, FloatType())

def make_uuid():
    """
    Generate UUID for each record
    """
    return F.udf(lambda: str(uuid.uuid1()), StringType())()

# Define schema for Reddit comments
cmt_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("up_votes", IntegerType(), True),
    StructField("down_votes", IntegerType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("timestamp", FloatType(), True),
    StructField("permalink", StringType(), True)
])

# Initialize Spark Session with optimized configurations
spark = (
    SparkSession
        .builder
        .appName("spark_streaming")
        .master("spark://spark-master-svc:7077")
        # Cassandra configurations
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.cassandra.auth.username", "cassandra")
        .config("spark.cassandra.auth.password", "cassandra")
        .config("spark.cassandra.output.consistency.level", "ONE")
        .getOrCreate()
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "redditcomments"

# Read from Kafka
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("maxOffsetsPerTrigger", "10000")
        .load()
)

# Parse JSON data
parsed_df = df.withColumn("comment_json", F.from_json(df["value"].cast("string"), cmt_schema))

# Select and transform columns
output_df = (
    parsed_df.select(
        "comment_json.id",
        "comment_json.name",
        "comment_json.author",
        "comment_json.body",
        "comment_json.subreddit",
        "comment_json.up_votes",
        "comment_json.down_votes",
        "comment_json.over_18",
        "comment_json.timestamp",
        "comment_json.permalink",
    )
    .withColumn("uuid", make_uuid())
    .withColumn("api_timestamp", F.from_unixtime(F.col("timestamp").cast(FloatType())))
    .withColumn("ingest_timestamp", F.current_timestamp())
    .drop("timestamp")
)

# Add sentiment score
output_df = output_df.withColumn(
    'sentiment_score', sentiment_udf(output_df['body'])
)

# Add watermark and repartition
output_df = (
    output_df
        .withWatermark("ingest_timestamp", "1 minute")
        .repartition("subreddit")
)

# Write main stream to Cassandra
main_query = (
    output_df
        .writeStream
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/check_point/comments/")
        .option("failOnDataLoss", "false")
        .format("org.apache.spark.sql.cassandra")
        .options(table="comments", keyspace="reddit")
        .start()
)

# Calculate and write summary statistics
summary_df = (
    output_df
        .withWatermark("ingest_timestamp", "1 minute")
        .groupBy("subreddit")
        .agg(F.avg("sentiment_score").alias("sentiment_score_avg"))
        .withColumn("uuid", make_uuid())
        .withColumn("ingest_timestamp", F.current_timestamp())
)

# Write summary stream to Cassandra
summary_query = (
    summary_df.writeStream
        .trigger(processingTime="5 seconds")
        .option("maxOffsetsPerTrigger", "10000")
        .option("checkpointLocation", "/tmp/check_point/summary/")
        .foreachBatch(
            lambda batchDF, batchID: (
                batchDF.write
                    .format("org.apache.spark.sql.cassandra")
                    .options(table="subreddit_sentiment_avg", keyspace="reddit")
                    .mode("append")
                    .save()
            )
        )
        .outputMode("update")
        .start()
)

spark.streams.awaitAnyTermination()
