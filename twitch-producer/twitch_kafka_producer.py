import os
import json
import time
import logging
from kafka import KafkaProducer
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'twitch_streams')
TWITCH_CLIENT_ID = os.environ.get('TWITCH_CLIENT_ID')
TWITCH_OAUTH_TOKEN = os.environ.get('TWITCH_OAUTH_TOKEN')

def create_producer():
    """Create and return a Kafka producer with retry logic"""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                api_version=(0, 10, 1),
                request_timeout_ms=30000,
                connections_max_idle_ms=540000
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count
            logger.error(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {str(e)}")
            logger.info(f"Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
    raise Exception("Could not connect to Kafka broker")

def get_twitch_streams():
    """Get top Twitch streams"""
    if not TWITCH_CLIENT_ID or not TWITCH_OAUTH_TOKEN:
        logger.error("Twitch credentials not found in environment variables")
        return []
    
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {TWITCH_OAUTH_TOKEN}'
    }
    
    try:
        # Get top streams
        url = 'https://api.twitch.tv/helix/streams?first=20'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('data', [])
        else:
            logger.error(f"Failed to fetch Twitch streams: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        logger.error(f"Error fetching Twitch data: {str(e)}")
        return []

def main():
    """Main function to run the Twitch stream producer"""
    logger.info("Starting Twitch Kafka Producer")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(15)
    
    # Create Kafka producer
    try:
        producer = create_producer()
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {str(e)}")
        return
    
    # Main loop
    try:
        while True:
            streams = get_twitch_streams()
            
            if streams:
                logger.info(f"Fetched {len(streams)} streams from Twitch")
                
                for stream in streams:
                    logger.info(f"Sending stream data for {stream.get('user_name')} to Kafka")
                    producer.send(TOPIC_NAME, value=stream)
                
                producer.flush()
                logger.info(f"Successfully sent {len(streams)} messages to Kafka")
            else:
                logger.warning("No streams retrieved from Twitch")
            
            # Wait before next fetch
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error in producer main loop: {str(e)}")
    finally:
        if producer:
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main()