from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType, FloatType
import uuid

def make_uuid():
    return F.udf(lambda: str(uuid.uuid1()), StringType())()

# define schema
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

spark = (
    SparkSession
        .builder
        .appName("spark_streaming")
        .master("spark://spark-master:7077")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.cassandra.output.consistency.level", "ONE")
        .getOrCreate()
)

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
KAFKA_TOPIC = "redditcomments"

# read from kafka
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .load()
)

parsed_df = df.withColumn("comment_json", F.from_json(df["value"].cast("string"), cmt_schema))

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

# https://stackoverflow.com/questions/64922560/pyspark-and-kafka-set-are-gone-some-data-may-have-been-missed
# adding failOnDataLoss as the checkpoint change with kafka brokers going down
(
    output_df
        .writeStream
        .option("checkpointLocation", "/tmp/check_point/")
        .option("failOnDataLoss", "false")
        .format("org.apache.spark.sql.cassandra")
        .options(table="comments", keyspace="reddit")
        .start()
)

spark.streams.awaitAnyTermination()

