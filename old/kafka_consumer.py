# kafka_consumer.py
# Kafka consumer module

import logging, json
from kafka import KafkaConsumer
logger = logging.getLogger(__name__)

def start_consumer(stop_event):
    # Replace with your topic name
    TOPIC = "price_ticks"

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["159.65.41.22:9092"],
        auto_offset_reset="latest",   
        enable_auto_commit=False,
        group_id=None,     # consumer group name
    )

    try:
        while not stop_event.is_set():
            try:
                records = consumer.poll(timeout_ms=1000)
                # if records:
                #     total = sum(len(msgs) for msgs in records.values())
                #     logger.info(f"Received {total} messages this poll")
                for tp, messages in records.items():
                    for message in messages:
                        try:
                            data = json.loads(message.value.decode("utf-8"))
                            price = data.get("price")
                            logger.info(f"Price: {price}")
                        except Exception:
                            logger.exception("Error processing message")
            except Exception:
                logger.exception("Consumer crashed during polling")
    except Exception:
        logger.exception("Consumer crashed")
    finally:
        try:
            consumer.close()
        except Exception:
            logger.exception("Error closing consumer during shutdown")
        logger.info("Kafka consumer shutting down.")
