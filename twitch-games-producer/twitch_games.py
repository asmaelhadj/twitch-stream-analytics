#!/usr/bin/env python3

import os
import json
import time
import logging
from kafka import KafkaProducer
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'twitch_games')
TWITCH_CLIENT_ID = os.environ.get('TWITCH_CLIENT_ID')
TWITCH_OAUTH_TOKEN = os.environ.get('TWITCH_OAUTH_TOKEN')

def create_producer():
    retry, max_retries = 0, 5
    while retry < max_retries:
        try:
            logger.info(f"Connecting to Kafka broker at {KAFKA_BROKER}")
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                api_version=(0, 10, 1),
                request_timeout_ms=30000,
                connections_max_idle_ms=540000
            )
        except Exception as e:
            retry += 1
            wait = 2 ** retry
            logger.error(f"Kafka connection failed ({retry}/{max_retries}): {e}")
            time.sleep(wait)
    raise Exception("Could not connect to Kafka broker")

def get_top_games():
    if not (TWITCH_CLIENT_ID and TWITCH_OAUTH_TOKEN):
        logger.error("Twitch credentials missing")
        return []
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {TWITCH_OAUTH_TOKEN}'
    }
    url = 'https://api.twitch.tv/helix/games/top?first=20'
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get('data', [])
    logger.error(f"Error fetching top games: {resp.status_code} {resp.text}")
    return []

def main():
    logger.info("Starting Twitch Games producer")
    time.sleep(15)
    producer = create_producer()
    try:
        while True:
            games = get_top_games()
            if games:
                logger.info(f"Fetched {len(games)} top games")
                for game in games:
                    producer.send(TOPIC_NAME, value=game)
                producer.flush()
            else:
                logger.warning("No games fetched")
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    main()