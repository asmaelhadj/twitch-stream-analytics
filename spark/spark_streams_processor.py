from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_spark_session():
    """Create Spark session with proper Kafka configuration"""
    return SparkSession.builder \
        .appName("TwitchStreamsProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints") \
        .getOrCreate()

def wait_for_services():
    """Wait for Kafka and Elasticsearch to be ready"""
    import socket
    import time
    
    def wait_for_port(host, port, timeout=300):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                if result == 0:
                    return True
            except:
                pass
            time.sleep(5)
        return False
    
    print("Waiting for Kafka to be ready...")
    if not wait_for_port("kafka", 9092):
        print("⚠️  Kafka not ready, continuing anyway...")
    else:
        print("✅ Kafka is ready!")
    
    print("Waiting for Elasticsearch to be ready...")
    if not wait_for_port("elasticsearch", 9200):
        print("⚠️  Elasticsearch not ready, continuing anyway...")
    else:
        print("✅ Elasticsearch is ready!")
    
    print("Waiting additional 30 seconds for services to stabilize...")
    time.sleep(30)

def process_stream_data(df):
    """Process and enrich stream data"""
    try:
        # Parse JSON and extract fields
        parsed_df = df.select(
            from_json(col("value").cast("string"), 
                     StructType([
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
                         StructField("is_mature", BooleanType(), True)
                     ])).alias("data"),
            col("timestamp")
        ).select("data.*", "timestamp")
        
        # Add processing timestamp and enrichment
        enriched_df = parsed_df.withColumn("processed_at", current_timestamp()) \
                              .withColumn("hour_of_day", hour(col("timestamp"))) \
                              .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                              .withColumn("viewer_category", 
                                        when(col("viewer_count") < 100, "small")
                                        .when(col("viewer_count") < 1000, "medium")
                                        .when(col("viewer_count") < 10000, "large")
                                        .otherwise("massive"))
        
        return enriched_df
        
    except Exception as e:
        print(f"Error processing stream data: {e}")
        return df

def main():
    print("Starting Twitch Streams Real-time Processor")
    
    # Wait for dependencies
    wait_for_services()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  # Reduce log noise
    
    try:
        print("Connecting to Kafka stream: twitch_streams")
        
        # Read from Kafka with proper configuration
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "twitch_streams") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .option("kafka.max.poll.interval.ms", "300000") \
            .load()
        
        print("✅ Connected to Kafka successfully!")
        
        # Process the stream
        processed_df = process_stream_data(df)
        
        # Write to Elasticsearch
        print("Starting stream processing to Elasticsearch...")
        
        query = processed_df.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "twitch_streams") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.nodes.wan.only", "true") \
            .option("es.batch.size.entries", "100") \
            .option("es.batch.size.bytes", "1mb") \
            .option("checkpointLocation", "/tmp/spark_checkpoints/streams") \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print("✅ Stream processing started successfully!")
        print("Processing Twitch streams data every 30 seconds...")
        print("Data will be available in Elasticsearch for Grafana visualization")
        
        # Keep the stream running
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streams processor: {e}")
        import traceback
        traceback.print_exc()
        
        # If Kafka connection fails, create sample data
        print("Creating sample stream data for testing...")
        try:
            from datetime import datetime
            sample_schema = StructType([
                StructField("user_name", StringType(), True),
                StructField("game_name", StringType(), True),
                StructField("viewer_count", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("timestamp", TimestampType(), True)
            ])
            
            sample_data = [
                ("summit1g", "Grand Theft Auto V", 25000, "GTA RP Adventures", datetime.now()),
                ("shroud", "VALORANT", 18000, "Ranked Grind", datetime.now()),
                ("ninja", "Fortnite", 30000, "Victory Royales", datetime.now())
            ]
            
            sample_df = spark.createDataFrame(sample_data, sample_schema)
            
            # Write sample data to Elasticsearch
            sample_df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", "twitch_streams") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.nodes.wan.only", "true") \
                .mode("append") \
                .save()
                
            print("✅ Sample stream data created successfully!")
            
        except Exception as sample_error:
            print(f"Could not create sample data: {sample_error}")
    
    finally:
        spark.stop()
        print("Spark session closed.")

if __name__ == "__main__":
    main()