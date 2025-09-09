# data.py
# Data pipeline module, used to get clickhouse data from the market data pipeline, create clickhouse clients for the portfolios, and get kafka data

import clickhouse_connect
from kafka import KafkaConsumer
import logging, json
logger = logging.getLogger(__name__)

# historical market data
def market_clickhouse_client():      
    client = clickhouse_connect.get_client(
        host="159.65.41.22",
        port=8123,
        username="default",   
        password="mysecurepassword",
        database="default"
    )
    return client
# portfolio data
def trading_clickhouse_client():
    return clickhouse_connect.get_client(
        # host="clickhouse",
        host="localhost",
        port=8123,
        username="default",
        password="mysecurepassword",
        database="default"
    )
# real time market data
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
def get_latest_price(consumer, timeout_ms=500):
    consumer.poll(timeout_ms=0)
    try:
        for tp in consumer.assignment():
            consumer.seek_to_end(tp)
        msg = next(consumer)
        return json.loads(msg.value.decode("utf-8")).get("price")
    except Exception:
        logger.exception("Error getting latest message from Kafka consumer")
        return None
    
# alternative "optimized" way to get latest message from the consumer, not currently implemented
# def get_latest_price(consumer, timeout_ms=1000):
#     try:
#         records = consumer.poll(timeout_ms=timeout_ms)
#         last = None
#         for tp, msgs in records.items():
#             if msgs:
#                 last = msgs[-1] if (last is None or msgs[-1].timestamp > last.timestamp) else last
#         return last 
#     except Exception:
#         logger.exception("Error getting latest message from Kafka consumer")
#         return None