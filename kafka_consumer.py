# kakfa_consumer.py
# kafka consumer that reads from the price_ticks topic and inserts into clickhouse

from kafka import KafkaConsumer
import json, time, logging
from datetime import datetime, timezone
logger = logging.getLogger(__name__)

# prepares the data for clickhouse
def validate_and_parse(data):

    timestamp_dt = datetime.fromisoformat(data['timestamp']).astimezone(timezone.utc)
    received_at_dt = datetime.fromisoformat(data['received_at']).astimezone(timezone.utc)

    # return a tuple in the exact order of table schema:
    return (
        timestamp_dt,                  # DateTime
        data['timestamp_ms'],          # Int64
        data['symbol'],                # String
        float(data['price']),          # Float64
        float(data['volume']),         # Float64
        received_at_dt                 # DateTime
    )

def start_consumer(stop_event):

    logger.info("Kafka consumer started.")
    
    consumer = KafkaConsumer(
        'price_ticks',
        bootstrap_servers=["159.65.41.22:9092"],
        group_id='ticks_ingestor',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        auto_offset_reset="earliest",   # add this
        consumer_timeout_ms=1000
    )

    try:
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=500) # wait for messages
            if not records:
                continue # if not records, continue to next iteration
            for tp, messages in records.items(): # get messages from each partition
                for message in messages:
                    logger.info(f"Message received: {message.value}")
    finally:

        logger.info("Closing consumer.")
        try:
            consumer.close()
        except Exception:
            logger.exception(f"Error closing consumer during shutdown")






