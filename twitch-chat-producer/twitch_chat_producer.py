import os
import json
import time
import socket
import logging
import random
from kafka import KafkaProducer
from datetime import datetime
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'twitch_chat')
IRC_CHANNELS = os.environ.get('IRC_CHANNELS', 'summit1g,shroud,ninja,xqcow,pokimane').split(',')

class TwitchChatProducer:
    def __init__(self):
        self.kafka_producer = None
        self.running = False
        
    def create_producer(self):
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

    def connect_to_twitch_irc(self):
        """Connect to Twitch IRC server"""
        try:
            server = 'irc.chat.twitch.tv'
            port = 6667
            nickname = 'justinfan12345'  # Anonymous user
            
            logger.info(f"Connecting to Twitch IRC: {server}:{port}")
            
            irc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            irc.connect((server, port))
            
            # Send authentication
            irc.send(f"PASS SCHMOOPIIE\n".encode('utf-8'))
            irc.send(f"NICK {nickname}\n".encode('utf-8'))
            
            # Join channels
            for channel in IRC_CHANNELS:
                channel = channel.strip()
                logger.info(f"Joining channel: #{channel}")
                irc.send(f"JOIN #{channel}\n".encode('utf-8'))
            
            logger.info("✅ Successfully connected to Twitch IRC")
            return irc
            
        except Exception as e:
            logger.error(f"Failed to connect to Twitch IRC: {e}")
            return None

    def parse_irc_message(self, message):
        """Parse IRC message and extract chat data"""
        try:
            if 'PRIVMSG' not in message:
                return None
                
            parts = message.split(':', 2)
            if len(parts) < 3:
                return None
                
            # Extract user info and message
            user_part = parts[1].split('!')[0]
            message_part = parts[2].strip()
            
            # Extract channel
            channel_part = parts[1].split('PRIVMSG')[1].strip().split()[0]
            channel = channel_part.replace('#', '')
            
            # Extract badges and user type info from IRC tags
            is_subscriber = 'subscriber' in parts[0] if len(parts) > 0 else False
            is_moderator = 'moderator' in parts[0] if len(parts) > 0 else False
            
            chat_data = {
                'channel': channel,
                'username': user_part,
                'message': message_part,
                'timestamp': datetime.now().isoformat(),
                'is_subscriber': is_subscriber,
                'is_moderator': is_moderator,
                'badges': []
            }
            
            return chat_data
            
        except Exception as e:
            logger.debug(f"Error parsing IRC message: {e}")
            return None

    def generate_sample_chat_messages(self):
        """Generate sample chat messages for testing"""
        sample_messages = [
            "Great gameplay!",
            "Pog",
            "Nice shot!",
            "Kappa",
            "LUL",
            "Amazing stream!",
            "GG",
            "What's the song?",
            "First time watching, loving it!",
            "Can you play my favorite game?",
            "OMEGALUL",
            "Poggers",
            "This is so good",
            "How long have you been streaming?",
            "Love the setup!"
        ]
        
        sample_usernames = [
            "viewer123", "chat_user", "stream_fan", "gamer_pro", "twitch_enjoyer",
            "sub_user", "longtime_follower", "new_viewer", "chat_lurker", "stream_regular"
        ]
        
        while self.running:
            try:
                for channel in IRC_CHANNELS:
                    channel = channel.strip()
                    
                    # Generate 1-3 messages per channel every cycle
                    num_messages = random.randint(1, 3)
                    
                    for _ in range(num_messages):
                        chat_data = {
                            'channel': channel,
                            'username': random.choice(sample_usernames),
                            'message': random.choice(sample_messages),
                            'timestamp': datetime.now().isoformat(),
                            'is_subscriber': random.choice([True, False]),
                            'is_moderator': random.choice([True, False]) if random.random() < 0.1 else False,
                            'badges': ['subscriber'] if random.choice([True, False]) else []
                        }
                        
                        if self.kafka_producer:
                            logger.info(f"Sending chat message from {chat_data['username']} in #{chat_data['channel']}")
                            self.kafka_producer.send(TOPIC_NAME, value=chat_data)
                
                if self.kafka_producer:
                    self.kafka_producer.flush()
                
                # Wait before generating more messages
                time.sleep(random.uniform(2, 8))  # Random delay between 2-8 seconds
                
            except Exception as e:
                logger.error(f"Error generating sample chat: {e}")
                time.sleep(5)

    def read_irc_messages(self, irc_socket):
        """Read messages from Twitch IRC"""
        buffer = ""
        
        while self.running:
            try:
                data = irc_socket.recv(2048).decode('utf-8')
                buffer += data
                
                # Process complete messages
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if line:
                        # Handle PING/PONG to keep connection alive
                        if line.startswith('PING'):
                            irc_socket.send(f"PONG {line.split()[1]}\n".encode('utf-8'))
                            continue
                        
                        # Parse chat message
                        chat_data = self.parse_irc_message(line)
                        if chat_data and self.kafka_producer:
                            logger.info(f"Real chat from {chat_data['username']} in #{chat_data['channel']}: {chat_data['message'][:50]}...")
                            self.kafka_producer.send(TOPIC_NAME, value=chat_data)
                            
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"Error reading IRC messages: {e}")
                break

    def run(self):
        """Main function to run the chat producer"""
        logger.info("Starting Twitch Chat Producer")
        
        # Wait for Kafka to be ready
        logger.info("Waiting for Kafka to be ready...")
        time.sleep(15)
        
        # Create Kafka producer
        try:
            self.kafka_producer = self.create_producer()
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            return
        
        self.running = True
        
        # Try to connect to Twitch IRC
        irc_socket = self.connect_to_twitch_irc()
        
        if irc_socket:
            logger.info("Using real Twitch IRC connection")
            try:
                irc_socket.settimeout(30)  # 30 second timeout
                
                # Start IRC reading in a separate thread
                irc_thread = threading.Thread(target=self.read_irc_messages, args=(irc_socket,))
                irc_thread.daemon = True
                irc_thread.start()
                
                # Also generate some sample messages for testing
                logger.info("Also generating sample messages for testing...")
                self.generate_sample_chat_messages()
                
            except Exception as e:
                logger.error(f"Error with IRC connection: {e}")
                logger.info("Falling back to sample chat generation...")
                self.generate_sample_chat_messages()
            finally:
                if irc_socket:
                    irc_socket.close()
        else:
            logger.info("Could not connect to IRC, using sample chat generation")
            self.generate_sample_chat_messages()

def main():
    producer = TwitchChatProducer()
    
    try:
        producer.run()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
        producer.running = False
    except Exception as e:
        logger.error(f"Error in producer: {str(e)}")
    finally:
        if producer.kafka_producer:
            producer.kafka_producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main()