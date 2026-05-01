"""
Workshop Lab 1: Rapid Kafka Diagnostics - "Silent Stall" Producer

This script simulates a real-world production anomaly known as a "Poison Pill."
1. It ensures the target Kafka topic exists with the required partition count.
2. It produces a steady stream of valid JSON order data.
3. After a configurable number of messages, it injects a single malformed message
   into a specific partition (Partition 2).

The malformed message contains a string ("ONE THOUSAND DOLLARS") in a field
where the consumer expects a numeric value, causing downstream processing to stall.

Usage:
    The script relies on environment variables for configuration, making it
    compatible with Docker Compose workshop environments.
"""

import time
import json
import os
import random

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configuration
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 3))
POISON_COUNT = int(os.getenv("POISON_COUNT", 100))  # When to trigger the stall


# Infrastructure Setup
def ensure_topic_exists():
    """
    Checks if the required topic exists; if not, creates it with the
    specified partition count and replication factor.
    """
    print(f"Checking/Creating topic '{TOPIC}' with {NUM_PARTITIONS} partitions...")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        new_topic = NewTopic(
            name=TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1
        )
        admin_client.create_topics([new_topic])
        print(f"Topic '{TOPIC}' created successfully.")
        admin_client.close()
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC}' already exists.")
    except Exception as e:
        print(f"Topic check result: {e}")


def on_send_error(excp):
    """Callback triggered if message delivery fails."""
    print(f"Message delivery failed: {excp}")


# Main Producer Logic
def run_producer():
    """
    Starts a continuous loop producing 'order' messages.
    At POISON_COUNT, it injects a malformed payload to trigger a consumer stall.
    """
    ensure_topic_exists()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Starting Producer to {TOPIC}...")

    message_count = 0
    poison_pill_sent = False

    try:
        while True:
            # 1. Prepare a standard "Good" Message
            order_id = f"ORD-{message_count}"
            data = {
                "order_id": order_id,
                "amount": round(random.uniform(10.0, 500.0), 2),
                "status": "NEW",
            }

            # Send valid data to a random partition
            future = producer.send(TOPIC, key=order_id, value=data)
            future.add_errback(on_send_error)

            # 2. Poison Pill Injection Logic
            if message_count == POISON_COUNT and not poison_pill_sent:
                print(
                    "\n!!! TRIGGERING LAB SCENARIO: INJECTING POISON PILL TO PARTITION 2 !!!"
                )

                # Malformed data: 'amount' is a string instead of a float/int
                poison_data = {
                    "order_id": "POISON-PILL",
                    "amount": "ONE THOUSAND DOLLARS",
                    "status": "CORRUPT",
                }

                p_future = producer.send(
                    TOPIC,
                    key="POISON",
                    value=poison_data,
                    partition=2,  # Target the specific partition for the lab
                )
                p_future.add_errback(on_send_error)
                poison_pill_sent = True

            message_count += 1

            # Control production speed (10 msg/sec)
            time.sleep(0.1)

            if message_count % 200 == 0:
                print(f"Total messages sent: {message_count}...")

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        # Final flush to clear the local producer queue before exiting
        print("Cleaning up and flushing remaining messages...")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_producer()
