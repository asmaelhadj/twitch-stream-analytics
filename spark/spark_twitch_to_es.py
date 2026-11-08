import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    trim, lower, when, year, month, dayofmonth, hour
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("Starting Spark Twitch to Elasticsearch Job")
print("Waiting for services to be ready...")
time.sleep(30)

# Create Spark session
spark = SparkSession.builder \
    .appName("TwitchToElasticsearch") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
print("Spark session created successfully")

# ───────────────────────────────────────────────────────────────
# SCHEMAS
# ───────────────────────────────────────────────────────────────
stream_schema = StructType([
    StructField("id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_login", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("viewer_count", IntegerType(), True),
    StructField("started_at", StringType(), True),
    StructField("language", StringType(), True),
    StructField("tag_ids", StringType(), True),
    StructField("is_mature", StringType(), True)
])

game_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

# ───────────────────────────────────────────────────────────────
# STREAMS PROCESSING
# ───────────────────────────────────────────────────────────────
def process_streams():
    print("Reading from topic: twitch_streams")
    raw_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "twitch_streams") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) AS json") \
        .select(from_json(col("json"), stream_schema).alias("data")) \
        .select("data.*")

    enhanced_df = parsed_stream_df \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("started_at_ts", to_timestamp(col("started_at"))) \
        .withColumn("is_mature", when(col("is_mature") == "true", True).otherwise(False)) \
        .withColumn("game_name", trim(lower(col("game_name")))) \
        .withColumn("title", trim(col("title"))) \
        .withColumn("year", year(col("started_at_ts"))) \
        .withColumn("month", month(col("started_at_ts"))) \
        .withColumn("day", dayofmonth(col("started_at_ts"))) \
        .withColumn("hour", hour(col("started_at_ts"))) \
        .withColumn("popularity_bucket",
            when(col("viewer_count") >= 10000, "Top Stream")
            .when(col("viewer_count") >= 5000, "Popular")
            .otherwise("Normal")
        ) \
        .fillna({
            "title": "Untitled Stream",
            "game_name": "unknown",
            "language": "unknown"
        })

    filtered_df = enhanced_df.filter(col("viewer_count") > 1000)

    def write_streams_to_es(batch_df, batch_id):
        try:
            if batch_df.count() > 0:
                print(f"[Streams] Writing batch {batch_id} to Elasticsearch")
                batch_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.resource", "twitch-streams-index/_doc") \
                    .option("es.mapping.id", "id") \
                    .option("es.write.operation", "upsert") \
                    .mode("append") \
                    .save()
        except Exception as e:
            print(f"[Streams] Error writing batch {batch_id}: {str(e)}")

    return filtered_df.writeStream \
        .foreachBatch(write_streams_to_es) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()

# ───────────────────────────────────────────────────────────────
# GAMES PROCESSING
# ───────────────────────────────────────────────────────────────
def process_games():
    print("Reading from topic: twitch_games")
    raw_games_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "twitch_games") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_games_df = raw_games_df.selectExpr("CAST(value AS STRING) AS json") \
        .select(from_json(col("json"), game_schema).alias("data")) \
        .select("data.id", col("data.name").alias("game_name"))

    def write_games_to_es(batch_df, batch_id):
        try:
            if batch_df.count() > 0:
                print(f"[Games] Writing batch {batch_id} to Elasticsearch")
                batch_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.resource", "twitch-games-index/_doc") \
                    .option("es.mapping.id", "id") \
                    .option("es.write.operation", "upsert") \
                    .mode("append") \
                    .save()
        except Exception as e:
            print(f"[Games] Error writing batch {batch_id}: {str(e)}")

    return parsed_games_df.writeStream \
        .foreachBatch(write_games_to_es) \
        .outputMode("update") \
        .trigger(processingTime="60 seconds") \
        .start()

# ───────────────────────────────────────────────────────────────
# RUN BOTH STREAMS
# ───────────────────────────────────────────────────────────────
try:
    print("Starting both streaming queries...")
    stream_query = process_streams()
    game_query = process_games()

    stream_query.awaitTermination()
    game_query.awaitTermination()

except Exception as e:
    print(f"Fatal Error in Spark Job: {str(e)}")
    import traceback
    traceback.print_exc()