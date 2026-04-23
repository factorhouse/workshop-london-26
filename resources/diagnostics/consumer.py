"""
Workshop Lab 1: Rapid Kafka Diagnostics - "Silent Stall" Consumer

This script simulates a common production failure where a consumer is
unable to process a specific message (the Poison Pill) and stops progressing.

Key Technical Behaviors:
1. Manual Commit: 'enable.auto.commit' is False. The consumer only commits
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
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from confluent_kafka import Consumer, TopicPartition

# Configuration
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
GROUP_ID = os.getenv("GROUP_ID", "orders-fulfillment")

# Consumer configuration
conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    # CRITICAL: Disable auto-commit.
    # This ensures we don't accidentally skip the bad message
    # without explicitly being told to by an operator.
    "enable.auto.commit": False,
}


def run_consumer():
    """
    Main consumer loop that processes messages and simulates a stall
    on malformed data.
    """
    c = Consumer(conf)
    c.subscribe([TOPIC])

    print(f"Consumer started. Group: {GROUP_ID}")
    print(f"Subscribed to topic: {TOPIC}")
    print("Waiting for messages and assignment...")

    try:
        while True:
            # Poll Kafka for new messages
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Business Logic / Processing
            try:
                payload = msg.value().decode("utf-8")
                data = json.loads(payload)

                # TRAP: DATA VALIDATION
                # We expect 'amount' to be a numeric value.
                # The 'Poison Pill' sent by the producer contains a string here.
                amount = data.get("amount")
                try:
                    # Attempt to parse as Decimal
                    amount_value = Decimal(str(amount)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                except (InvalidOperation, TypeError):
                    # This raises an error when the Poison Pill is encountered
                    raise ValueError(
                        f"Validation Failed! 'amount' must be numeric, got: {amount}"
                    )

                # If processing succeeds:
                print(
                    f"[OK] Partition {msg.partition()} | Offset {msg.offset()} | "
                    f"Order: {data['order_id']} | Amount: {amount_value}"
                )

                # Manually commit the offset only after successful processing
                c.commit(message=msg, asynchronous=False)

            except (InvalidOperation, TypeError, ValueError, KeyError) as e:
                # STALL MECHANISM
                # The application logic fails here. We log the coordinates
                # so the operator can use Kpow to investigate.
                print("\n[!!!] PROCESSING FAILED [!!!]")
                print(f"Location: Partition {msg.partition()}, Offset {msg.offset()}")
                print("Unknown error is encountered!")
                # print(f"Technical Error: {e}")
                print("Action: System retry initiated. Retrying in 2 seconds...")

                # Seek back to the current offset to attempt re-processing
                tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
                c.seek(tp)

                # Wait before the next loop iteration (simulates a backoff/retry)
                time.sleep(2)

    except KeyboardInterrupt:
        print("\nConsumer stopping...")
    finally:
        # Final cleanup to leave the group cleanly
        c.close()


if __name__ == "__main__":
    run_consumer()
