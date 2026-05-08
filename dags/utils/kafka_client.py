import json
import logging
import os
from kafka import KafkaProducer

def get_kafka_producer():
    """
    Initializes and returns a Kafka producer.
    In Docker, the bootstrap server is 'kafka:9092'.
    """
    try:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Retries and reliability
            retries=5
        )
        return producer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def send_news_event(article_data: dict):
    """
    Sends an article event to the 'news_events' Kafka topic.
    """
    producer = get_kafka_producer()
    if not producer:
        return

    try:
        topic = "news_events"
        # We only send metadata for the event to keep it lightweight
        event = {
            "event_type": "ARTICLE_SCRAPED",
            "source": article_data.get("source"),
            "title": article_data.get("title"),
            "url": article_data.get("url"),
            "timestamp": article_data.get("scraped_at")
        }
        
        producer.send(topic, event)
        producer.flush()
        logging.info(f"Sent event to Kafka topic '{topic}': {article_data.get('title')}")
    except Exception as e:
        logging.error(f"Error sending event to Kafka: {e}")
    finally:
        producer.close()
