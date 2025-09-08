# data_engine.py
# Data pipeline module

import clickhouse_connect
from kafka import KafkaConsumer
import logging
logger = logging.getLogger(__name__)

# clickhouse clients for both market and portfolio data, and kafka consumer for market data

def market_clickhouse_client():      
    client = clickhouse_connect.get_client(
        host="159.65.41.22",
        port=8123,
        username="default",   
        password="mysecurepassword",
        database="default"
    )
    return client

def trading_clickhouse_client():
    return clickhouse_connect.get_client(
        # host="clickhouse",
        host="localhost",
        port=8123,
        username="default",
        password="mysecurepassword",
        database="default"
    )

def get_kafka_data(kafka_topic):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=["159.65.41.22:9092"],
        auto_offset_reset="latest",   
        enable_auto_commit=False,
        group_id=None,
    )
    return consumer

# return latest message from the consumer
def get_latest_price(consumer):
    consumer.poll(timeout_ms=0)
    try:
        for tp in consumer.assignment():
            consumer.seek_to_end(tp)
        msg = next(consumer)
        return msg
    except Exception:
        logger.exception("Error getting latest message from Kafka consumer")
        return None