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

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Configuration
# Variables are pulled from the environment with sensible defaults for local dev.
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 3))
POISON_COUNT = int(os.getenv("POISON_COUNT", 100))  # When to trigger the stall

# Shared configuration for Kafka clients
conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}


# Infrastructure Setup
def ensure_topic_exists():
    """
    Checks if the required topic exists; if not, creates it with the
    specified partition count and replication factor.
    """
    print(f"Checking/Creating topic '{TOPIC}' with {NUM_PARTITIONS} partitions...")
    admin_client = AdminClient(conf)

    # Define the new topic configuration
    new_topic = NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)

    # Trigger creation (non-blocking call)
    fs = admin_client.create_topics([new_topic])

    # Wait for the result of the creation
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            # We catch errors (e.g., TopicAlreadyExists) and continue
            print(f"Topic check result: {e}")


def delivery_report(err, msg):
    """
    Callback triggered by the Producer once a message is successfully delivered
    or failed to be sent to the broker.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")


# Main Producer Logic
def run_producer():
    """
    Starts a continuous loop producing 'order' messages.
    At POISON_COUNT, it injects a malformed payload to trigger a consumer stall.
    """
    ensure_topic_exists()

    producer = Producer(conf)
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
            producer.produce(
                TOPIC, key=order_id, value=json.dumps(data), callback=delivery_report
            )

            # 2. Poison Pill Injection Logic
            # We target Partition 2 specifically to simulate a localized partition stall.
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

                producer.produce(
                    TOPIC,
                    key="POISON",
                    value=json.dumps(poison_data),
                    partition=2,  # Target the specific partition for the lab
                    callback=delivery_report,
                )
                poison_pill_sent = True

            # Serve delivery callbacks from previous produce calls
            producer.poll(0)
            message_count += 1

            # Control production speed (10 msg/sec)
            time.sleep(0.1)

            # Periodic log and flush to ensure data is moving
            if message_count % 10 == 0:
                producer.flush()

            if message_count % 200 == 0:
                print(f"Total messages sent: {message_count}...")

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        # Final flush to clear the local producer queue before exiting
        print("Cleaning up and flushing remaining messages...")
        producer.flush()


if __name__ == "__main__":
    run_producer()
