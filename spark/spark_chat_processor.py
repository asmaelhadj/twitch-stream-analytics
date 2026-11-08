from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_spark_session():
    """Create Spark session with proper Kafka configuration"""
    return SparkSession.builder \
        .appName("TwitchChatProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/chat_checkpoints") \
        .config("spark.es.index.auto.create", "true") \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.nodes.discovery", "false") \
        .config("spark.es.http.timeout", "5m") \
        .config("spark.es.http.retries", "3") \
        .config("spark.es.batch.write.retry.count", "3") \
        .config("spark.es.batch.write.retry.wait", "10s") \
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

def test_elasticsearch_connection():
    """Test Elasticsearch connection"""
    try:
        # Simple socket test for Elasticsearch
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex(("elasticsearch", 9200))
        sock.close()
        
        if result == 0:
            print("✅ Elasticsearch is accessible!")
            return True
        else:
            print("⚠️  Could not connect to Elasticsearch")
            return False
            
    except Exception as e:
        print(f"⚠️  Could not test Elasticsearch connection: {e}")
        return False

def process_chat_data(df):
    """Process and analyze chat data"""
    try:
        # Parse JSON chat messages
        parsed_df = df.select(
            from_json(col("value").cast("string"), 
                     StructType([
                         StructField("channel", StringType(), True),
                         StructField("username", StringType(), True),
                         StructField("message", StringType(), True),
                         StructField("timestamp", StringType(), True),
                         StructField("is_subscriber", BooleanType(), True),
                         StructField("is_moderator", BooleanType(), True),
                         StructField("badges", ArrayType(StringType()), True)
                     ])).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Add chat analytics
        enriched_df = parsed_df.withColumn("processed_at", current_timestamp()) \
                              .withColumn("message_length", length(col("message"))) \
                              .withColumn("word_count", size(split(col("message"), " "))) \
                              .withColumn("hour_of_day", hour(col("kafka_timestamp"))) \
                              .withColumn("has_emotes", 
                                        col("message").contains("Kappa") |
                                        col("message").contains("LUL") |
                                        col("message").contains("Pog") |
                                        col("message").contains("OMEGALUL")) \
                              .withColumn("user_type", 
                                        when(col("is_moderator"), "moderator")
                                        .when(col("is_subscriber"), "subscriber")
                                        .otherwise("viewer"))
        
        return enriched_df
        
    except Exception as e:
        print(f"Error processing chat data: {e}")
        return df

def write_to_elasticsearch_batch(df, batch_id):
    """Custom function to write each batch to Elasticsearch"""
    try:
        print(f"Processing chat batch {batch_id} with {df.count()} records")
        
        if df.count() > 0:
            # Write to Elasticsearch with proper error handling
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", "twitch_chat") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.nodes.wan.only", "true") \
                .option("es.nodes.discovery", "false") \
                .option("es.index.auto.create", "true") \
                .option("es.batch.size.entries", "50") \
                .option("es.batch.size.bytes", "1mb") \
                .option("es.http.timeout", "5m") \
                .option("es.batch.write.retry.count", "3") \
                .option("es.batch.write.retry.wait", "10s") \
                .mode("append") \
                .save()
            
            print(f"✅ Successfully wrote chat batch {batch_id} to Elasticsearch")
        else:
            print(f"⚠️  Chat batch {batch_id} is empty, skipping...")
            
    except Exception as e:
        print(f"❌ Error writing chat batch {batch_id} to Elasticsearch: {e}")
        import traceback
        traceback.print_exc()

def create_sample_chat_data(spark):
    """Create sample chat data if no Kafka stream is available"""
    print("Creating sample chat data for testing...")
    try:
        from datetime import datetime
        sample_schema = StructType([
            StructField("channel", StringType(), True),
            StructField("username", StringType(), True),
            StructField("message", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("is_subscriber", BooleanType(), True),
            StructField("is_moderator", BooleanType(), True),
            StructField("badges", ArrayType(StringType()), True),
            StructField("processed_at", TimestampType(), True),
            StructField("message_length", IntegerType(), True),
            StructField("word_count", IntegerType(), True),
            StructField("user_type", StringType(), True),
            StructField("has_emotes", BooleanType(), True)
        ])
        
        sample_data = [
            ("summit1g", "viewer123", "Great gameplay!", datetime.now(), False, False, [], datetime.now(), 15, 2, "viewer", False),
            ("shroud", "sub_user", "Amazing shots Kappa", datetime.now(), True, False, ["subscriber"], datetime.now(), 19, 3, "subscriber", True),
            ("ninja", "mod_user", "Keep it clean chat", datetime.now(), False, True, ["moderator"], datetime.now(), 18, 4, "moderator", False),
            ("pokimane", "chat_fan", "Love this stream!", datetime.now(), False, False, [], datetime.now(), 18, 3, "viewer", False),
            ("xqcow", "longtime_viewer", "Poggers gameplay LUL", datetime.now(), True, False, ["subscriber"], datetime.now(), 20, 3, "subscriber", True),
            ("summit1g", "new_viewer", "First time watching", datetime.now(), False, False, [], datetime.now(), 19, 3, "viewer", False),
            ("shroud", "stream_regular", "OMEGALUL that was funny", datetime.now(), False, False, [], datetime.now(), 24, 4, "viewer", True),
            ("ninja", "supporter", "Keep up the great content!", datetime.now(), True, False, ["subscriber"], datetime.now(), 28, 5, "subscriber", False)
        ]
        
        sample_df = spark.createDataFrame(sample_data, sample_schema)
        
        # Write sample data to Elasticsearch using the batch function
        write_to_elasticsearch_batch(sample_df, "sample_chat")
        
        print("✅ Sample chat data created successfully!")
        return True
        
    except Exception as sample_error:
        print(f"❌ Could not create sample chat data: {sample_error}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("Starting Twitch Chat Real-time Processor")
    
    # Wait for dependencies
    wait_for_services()
    
    # Test Elasticsearch connection and create index
    es_ready = test_elasticsearch_connection()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  # Reduce log noise
    
    try:
        print("Connecting to Kafka stream: twitch_chat")
        
        # Read from Kafka with proper configuration
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "twitch_chat") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .option("kafka.max.poll.interval.ms", "300000") \
            .option("kafka.consumer.poll.ms", "1000") \
            .load()
        
        print("✅ Connected to Kafka successfully!")
        
        # Process the stream
        processed_df = process_chat_data(df)
        
        # Write to Elasticsearch using foreachBatch for better error handling
        print("Starting chat processing to Elasticsearch...")
        
        query = processed_df.writeStream \
            .foreachBatch(write_to_elasticsearch_batch) \
            .option("checkpointLocation", "/tmp/chat_checkpoints/chat") \
            .outputMode("append") \
            .trigger(processingTime="15 seconds") \
            .start()
        
        print("✅ Chat processing started successfully!")
        print("Processing Twitch chat data every 15 seconds...")
        print("Chat analytics will be available in Elasticsearch")
        print("You can check the index at: http://localhost:9200/twitch_chat/_search")
        
        # Keep the stream running for a reasonable time, then check if data is coming
        import threading
        
        def monitor_chat_stream():
            time.sleep(120)  # Wait 2 minutes
            print("Checking if chat stream is active...")
            try:
                # Use simple socket check instead of requests
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(("elasticsearch", 9200))
                sock.close()
                
                if result == 0:
                    print("✅ Elasticsearch is still accessible")
                    print("Chat data should be flowing to the twitch_chat index")
                    print("Check Kibana at http://localhost:5601 to see the data")
                else:
                    print("⚠️  Cannot reach Elasticsearch")
                    print("Creating sample data as fallback...")
                    create_sample_chat_data(spark)
            except Exception as e:
                print(f"Error monitoring chat stream: {e}")
                print("Creating sample data as fallback...")
                create_sample_chat_data(spark)
        
        # Start monitoring in background
        monitor_thread = threading.Thread(target=monitor_chat_stream)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Keep the stream running
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in chat processor: {e}")
        import traceback
        traceback.print_exc()
        
        # If Kafka connection fails, create sample data
        print("Kafka chat stream failed, creating sample data for testing...")
        create_sample_chat_data(spark)
    
    finally:
        spark.stop()
        print("Spark session closed.")

if __name__ == "__main__":
    main()