"""
Workshop Lab 1: Rapid Kafka Diagnostics - "Silent Stall" Consumer

This script simulates a common production failure where a consumer is
unable to process a specific message (the Poison Pill) and stops progressing.

Key Technical Behaviors:
1. Manual Commit: 'enable_auto_commit' is False. The consumer only commits
   offsets after successful processing.
2. Schema Validation: The script attempts to convert the 'amount' field
   to a Decimal.
3. Infinite Retry Loop: When it encounters the Poison Pill (where 'amount'
   is a string), it performs a 'seek' to the current offset and retries.
4. The "Zombie" State: Because it continues to call 'poll()', it sends
   heartbeats to the broker. The Consumer Group appears "Healthy" in basic
   monitoring, but 'Consumer Lag' will continue to grow on the affected partition.
"""

import json
import os
import re
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

# Configuration
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
SASL_JAAS_CONFIG = os.getenv("SASL_JAAS_CONFIG", "")
TOPIC = os.getenv("TOPIC", "orders")
GROUP_ID = os.getenv("GROUP_ID", "orders-fulfillment")


def get_kafka_kwargs():
    """
    Constructs the Kafka connection arguments based on environment variables.
    Handles extraction of credentials from SASL_JAAS_CONFIG if authentication is enabled.
    """
    kafka_kwargs = {"bootstrap_servers": BOOTSTRAP}

    if SECURITY_PROTOCOL:
        kafka_kwargs["security_protocol"] = SECURITY_PROTOCOL
        kafka_kwargs["sasl_mechanism"] = SASL_MECHANISM

        # kafka-python doesn't parse JAAS strings automatically, so we extract the credentials manually
        jaas_config = SASL_JAAS_CONFIG
        if jaas_config:
            user_match = re.search(r'username="([^"]+)"', jaas_config)
            pass_match = re.search(r'password="([^"]+)"', jaas_config)

            if user_match and pass_match:
                kafka_kwargs["sasl_plain_username"] = user_match.group(1)
                kafka_kwargs["sasl_plain_password"] = pass_match.group(1)

    return kafka_kwargs


def run_consumer():
    """
    Main consumer loop that processes messages and simulates a stall
    on malformed data.
    """
    c = KafkaConsumer(
        TOPIC,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        # CRITICAL: Disable auto-commit.
        # This ensures we don't accidentally skip the bad message
        # without explicitly being told to by an operator.
        enable_auto_commit=False,
        **get_kafka_kwargs(),
    )

    print(f"Consumer started. Group: {GROUP_ID}")
    print(f"Subscribed to topic: {TOPIC}")
    print("Waiting for messages and assignment...")

    try:
        while True:
            # Poll Kafka for new messages (returns a dict grouped by TopicPartition)
            records = c.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    processing_failed = False

                    # 1. Business Logic / Processing
                    try:
                        payload = msg.value.decode("utf-8")
                        data = json.loads(payload)

                        # TRAP: DATA VALIDATION
                        amount = data.get("amount")

                        # Attempt to parse as Decimal
                        amount_value = Decimal(str(amount)).quantize(
                            Decimal("0.01"), rounding=ROUND_HALF_UP
                        )

                    except (InvalidOperation, TypeError, ValueError, KeyError):
                        # Catch only data-related exceptions so we don't swallow Kafka errors
                        processing_failed = True

                    # 2. STALL MECHANISM
                    if processing_failed:
                        print("\n[!!!] PROCESSING FAILED [!!!]")
                        print(
                            f"Location: Partition {msg.partition}, Offset {msg.offset}"
                        )
                        print("Unknown error is encountered!")
                        print(
                            "Action: System retry initiated. Retrying in 2 seconds..."
                        )

                        # Seek back to the current offset to attempt re-processing
                        c.seek(tp, msg.offset)

                        # Wait before the next loop iteration
                        time.sleep(2)

                        # Break out of the message loop for this partition so we re-poll
                        # the failed offset instead of processing subsequent messages
                        break

                    # 3. SUCCESS & COMMIT
                    print(
                        f"[OK] Partition {msg.partition} | Offset {msg.offset} | "
                        f"Order: {data['order_id']} | Amount: {amount_value}"
                    )

                    # Manually commit the offset only after successful processing.
                    # Handle varying OffsetAndMetadata signatures across kafka-python versions.
                    try:
                        # kafka-python >= 2.3.1 requires leader_epoch (-1 indicates unknown)
                        offset_meta = OffsetAndMetadata(msg.offset + 1, "", -1)
                    except TypeError:
                        # Older kafka-python versions
                        offset_meta = OffsetAndMetadata(msg.offset + 1, "")

                    c.commit({tp: offset_meta})

    except KeyboardInterrupt:
        print("\nConsumer stopping...")
    finally:
        # Final cleanup to leave the group cleanly
        c.close()


if __name__ == "__main__":
    run_consumer()
