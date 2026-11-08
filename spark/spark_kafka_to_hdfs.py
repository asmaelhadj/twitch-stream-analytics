from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, ArrayType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for Kafka JSON messages based on sample data
schema = StructType([
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
    StructField("thumbnail_url", StringType(), True),
    StructField("tag_ids", ArrayType(StringType()), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("is_mature", BooleanType(), True)
])

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToHDFSWriter") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

# Set log level for Spark
spark.sparkContext.setLogLevel("INFO")

try:
    # Read from Kafka
    logger.info("Connecting to Kafka at kafka:9092, topic: twitch_streams")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "twitch_streams") \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract value as JSON string
    parsed_df = df.selectExpr("CAST(value AS STRING) as json")

    # Parse JSON string into structured columns
    logger.info("Parsing JSON data")
    structured_df = parsed_df.select(
        from_json(col("json"), schema).alias("data")
    ).select("data.*")

    # Preprocessing: Filter invalid records
    cleaned_df = structured_df.filter(
        (col("user_name").isNotNull()) &
        (col("viewer_count").isNotNull()) &
        (col("viewer_count") >= 0)
    )

    # Preprocessing: Standardize timestamps and add processed timestamp
    enriched_df = cleaned_df.withColumn(
        "timestamp", 
        to_timestamp(col("started_at"))  # Convert ISO 8601 string to timestamp
    ).withColumn(
        "processed_timestamp", 
        current_timestamp()
    ).withColumn(
        "date", 
        to_date(col("timestamp"))  # For partitioning
    )

    # Preprocessing: Select relevant fields, including is_mature
    final_df = enriched_df.select(
        "user_name",
        "game_name",
        "viewer_count",
        "timestamp",
        "title",
        "language",
        "is_mature",
        "processed_timestamp",
        "date"
    )

    # Write to HDFS as JSON with partitioning
    logger.info("Writing to HDFS at hdfs://namenode:8020/data/processed_twitch_streams")
    query = final_df.writeStream \
        .format("json") \
        .option("path", "hdfs://namenode:8020/data/processed_twitch_streams") \
        .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/processed_twitch_streams_checkpoint") \
        .partitionBy("date") \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .start()

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in streaming job: {str(e)}")
    raise