from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToHDFSWriter") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitch_streams") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract value as JSON string
parsed_df = df.selectExpr("CAST(value AS STRING) as json")

# Write raw messages to HDFS
query = parsed_df.writeStream \
    .format("text") \
    .option("path", "hdfs://namenode:8020/data/raw_twitch_streams") \
    .option("checkpointLocation", "/tmp/hdfs_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
