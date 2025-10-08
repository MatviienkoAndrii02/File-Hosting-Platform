from confluent_kafka import Producer
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def send_event(topic: str, key: str, value: dict):
    import json
    producer.produce(topic, key=key, value=json.dumps(value))
    producer.flush()