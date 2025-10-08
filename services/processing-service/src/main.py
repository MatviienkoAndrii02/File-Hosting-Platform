import os
import json
import io
import time
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from services.minio_client import minio_client, MINIO_BUCKET
from utils import create_thumbnail
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

logging.info("Starting Kafka producer...")
producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def send_file_processed_event(file_info: dict, processed_filename: str):
    event = {
        "file_id": file_info["file_id"],
        "filename": file_info["filename"],
        "processed_filename": processed_filename,
        "owner_id": file_info["owner_id"]
    }
    try:
        producer.produce("file_processed", json.dumps(event).encode("utf-8"))
        producer.flush()
        logging.info(f"➡️ Sent file_processed event for {file_info['filename']}")
    except KafkaException as e:
        logging.info(f"Failed to send file_processed event: {e}")


def create_consumer():
    while True:
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BROKER,
                "group.id": "processing-service-group",
                "auto.offset.reset": "earliest"
            })

            while True:
                metadata = consumer.list_topics(timeout=5)
                if "file_uploaded" in metadata.topics:
                    break

            consumer.subscribe(["file_uploaded"])
            logging.info("Consumer created and subscribed to 'file_uploaded'")
            return consumer
        except KafkaException as e:
            logging.info(f"Kafka consumer connection failed: {e}, retrying in 5s...")
            time.sleep(5)


def process_file(file_info: dict):
    filename = file_info["filename"]
    try:
        logging.info(f"🔄 Processing file: {filename}")
        response = minio_client.get_object(MINIO_BUCKET, filename)
        file_bytes = response.read()

        processed_bytes = create_thumbnail(file_bytes)
        processed_filename = f"processed/{filename}"

        minio_client.put_object(
            MINIO_BUCKET,
            processed_filename,
            data=io.BytesIO(processed_bytes),
            length=len(processed_bytes),
            content_type="image/jpeg"
        )

        send_file_processed_event(file_info, processed_filename)
        logging.info(f"Finished processing: {filename}")

    except Exception as e:
        logging.info(f"Error processing file {filename}: {e}")


def run():
    logging.info("Processing service started...")
    consumer = create_consumer()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.info(f"Consumer error: {msg.error()}")
                continue

            try:
                file_info = json.loads(msg.value().decode("utf-8"))
                logging.info(f"Got message: {file_info}")
                process_file(file_info)
            except Exception as e:
                logging.info(f"Failed to decode/process message: {e}")

    finally:
        consumer.close()
        logging.info("Consumer closed")


if __name__ == "__main__":
    run()
