#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, expr, when, ceil, to_json, struct, size
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_INPUT_TOPIC = 'emoji-events'
KAFKA_OUTPUT_TOPIC = 'aggregated-emoji-topic'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

logger.info("Spark session started")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

logger.info(f"Reading from Kafka topic: {KAFKA_INPUT_TOPIC}")


emoji_df = df.selectExpr("CAST(value AS STRING) as json_string")
emoji_df_parsed = emoji_df.select(
    expr("json_tuple(json_string, 'user_id', 'emoji_type', 'timestamp')").alias("user_id", "emoji_type", "timestamp")
).withColumn("timestamp", col("timestamp").cast(TimestampType()))

aggregated_df = emoji_df_parsed \
    .groupBy(
        window(col("timestamp"), "2 seconds"),
        "emoji_type"
    ) \
    .agg(
        count("*").alias("emoji_count"),
        expr("collect_set(user_id)").alias("unique_users")
    ) \
    .withColumn(
        "normalized_count",
        when(col("emoji_count") >= 20, ceil(col("emoji_count") / 100)).otherwise(0)
    )

output_df = aggregated_df.select(
    to_json(struct(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        "emoji_type",
        "emoji_count",
        "normalized_count",
        size("unique_users").alias("unique_user_count")
    )).alias("value")
)

# Write the aggregated data to the Kafka output topic
query = output_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", KAFKA_OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

logger.info(f"Writing to Kafka topic: {KAFKA_OUTPUT_TOPIC}")

query.awaitTermination()