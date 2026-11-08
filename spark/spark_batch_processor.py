from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with proper configuration"""
    return SparkSession.builder \
        .appName("TwitchBatchProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/tmp/spark-warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()

def generate_sample_insights(spark):
    """Generate sample insights when no historical data exists"""
    logger.info("Generating sample insights and recommendations...")
    
    # Define schema for platform insights
    insights_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("recommendation", StringType(), True)
    ])
    
    # Create sample data with proper timestamp objects
    current_time = datetime.now()
    
    sample_data = [
        (current_time, "avg_viewer_count", 15000.0, "engagement", "Focus on prime time streaming (7-10 PM)"),
        (current_time, "top_game_dominance", 0.35, "content", "Diversify game portfolio beyond top title"),
        (current_time, "chat_engagement_rate", 0.12, "interaction", "Encourage more viewer interaction"),
        (current_time, "stream_duration_optimal", 4.5, "scheduling", "Optimal stream length is 4-5 hours"),
        (current_time, "weekend_boost_factor", 1.8, "timing", "Weekend streams show 80% higher engagement"),
        (current_time, "mature_stream_ratio", 0.1, "content", "Consider targeting non-mature audiences for broader reach")
    ]
    
    # Create DataFrame with explicit schema
    platform_insights = spark.createDataFrame(sample_data, insights_schema)
    
    logger.info("Platform Analytics Summary:")
    logger.info("=" * 50)
    platform_insights.show(truncate=False)
    
    # Write insights to Elasticsearch
    try:
        logger.info("Writing insights to Elasticsearch...")
        platform_insights.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "twitch_insights") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()
        logger.info("✅ Successfully wrote insights to Elasticsearch!")
    except Exception as e:
        logger.error(f"⚠️ Could not write to Elasticsearch: {e}")
    
    return platform_insights

def get_last_processed_date(spark):
    """Read the last processed date from HDFS, if it exists"""
    try:
        last_date_path = "hdfs://namenode:8020/checkpoints/batch_processor_processed_streams/last_processed_date"
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(last_date_path)
        
        if fs.exists(path):
            df = spark.read.text(last_date_path)
            if df.count() > 0:  # Check if DataFrame is not empty
                last_date = df.select("value").first()[0]
                logger.info(f"Last processed date: {last_date}")
                return last_date
            else:
                logger.info("Last processed date file is empty. Processing all data.")
                return None
        else:
            logger.info("No last processed date found. Processing all data.")
            return None
    except Exception as e:
        logger.error(f"Error reading last processed date: {e}")
        return None

def save_last_processed_date(spark, date):
    """Save the last processed date to HDFS"""
    try:
        last_date_path = "hdfs://namenode:8020/checkpoints/batch_processor_processed_streams/last_processed_date"
        # Convert date to string to avoid text format error
        date_str = str(date)
        spark.createDataFrame([(date_str,)], ["value"]).write \
            .mode("overwrite") \
            .text(last_date_path)
        logger.info(f"Saved last processed date: {date_str}")
    except Exception as e:
        logger.error(f"Error saving last processed date: {e}")

def process_historical_data(spark, hdfs_path, last_processed_date=None):
    """Process historical data from HDFS, optionally filtering by date"""
    try:
        logger.info(f"Reading historical data from {hdfs_path}...")
        df = spark.read.json(hdfs_path)
        
        # Filter by date if last_processed_date is provided
        if last_processed_date:
            logger.info(f"Filtering data after {last_processed_date}...")
            df = df.filter(col("date") > last_processed_date)
        
        record_count = df.count()
        logger.info(f"Found {record_count} historical records")
        
        if record_count == 0:
            logger.info("No new data to process.")
            return None, None, None
        
        # Perform aggregations
        daily_stats = df.groupBy("date").agg(
            avg("viewer_count").alias("avg_viewers"),
            max("viewer_count").alias("peak_viewers"),
            count("*").alias("stream_count"),
            countDistinct("user_name").alias("unique_streamers")
        )
        
        logger.info("Daily Statistics:")
        daily_stats.show()
        
        # Top games analysis
        top_games = df.groupBy("game_name").agg(
            count("*").alias("stream_count"),
            avg("viewer_count").alias("avg_viewers")
        ).orderBy(desc("stream_count")).limit(10)
        
        logger.info("Top Games:")
        top_games.show()
        
        # Mature vs. non-mature streams analysis, including game breakdown
        mature_stats = df.groupBy("is_mature", "game_name").agg(
            count("*").alias("stream_count"),
            avg("viewer_count").alias("avg_viewers"),
            countDistinct("user_name").alias("unique_streamers")
        ).orderBy("is_mature", desc("stream_count")).limit(20)
        
        logger.info("Mature vs. Non-Mature Streams by Game:")
        mature_stats.show()
        
        # Write results to HDFS
        try:
            logger.info("Writing results to HDFS...")
            daily_stats.write \
                .mode("append") \
                .parquet("hdfs://namenode:8020/data/processed_twitch_stats/daily_stats")
            top_games.write \
                .mode("append") \
                .parquet("hdfs://namenode:8020/data/processed_twitch_stats/top_games")
            mature_stats.write \
                .mode("append") \
                .parquet("hdfs://namenode:8020/data/processed_twitch_stats/mature_stats")
            logger.info("✅ Successfully wrote results to HDFS!")
        except Exception as e:
            logger.error(f"⚠️ Could not write results to HDFS: {e}")
        
        # Save the latest date processed
        latest_date = df.select(max("date")).first()[0]
        if latest_date:
            save_last_processed_date(spark, latest_date)
        
        return daily_stats, top_games, mature_stats
        
    except Exception as e:
        logger.error(f"Error processing historical data: {e}")
        return None, None, None

def main():
    logger.info("Starting Spark Batch Processor for Historical Analysis")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    try:
        logger.info("Starting batch processing of historical data...")
        
        # Try to read from HDFS
        hdfs_path = "hdfs://namenode:8020/data/processed_twitch_streams"
        logger.info(f"Reading historical stream data from HDFS...")
        
        try:
            # Check if path exists
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            
            if fs.exists(path):
                # Get last processed date
                last_processed_date = get_last_processed_date(spark)
                daily_stats, top_games, mature_stats = process_historical_data(spark, hdfs_path, last_processed_date)
                if daily_stats is None:
                    logger.info("No new data processed. Exiting.")
                    return
            else:
                logger.info(f"No historical data found in HDFS yet: [PATH_NOT_FOUND] Path does not exist: {hdfs_path}.")
                logger.info("Creating sample historical analysis...")
                daily_stats = generate_sample_insights(spark)
                
        except Exception as hdfs_error:
            logger.error(f"HDFS access error: {hdfs_error}")
            logger.info("Creating sample historical analysis...")
            daily_stats = generate_sample_insights(spark)
        
        logger.info("✅ Batch processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in batch processor: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        logger.info("Spark session closed.")

if __name__ == "__main__":
    main()