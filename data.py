# data.py
# used to get kafka data, clickhouse data, and connect to aws

from config import MARKET_DATA_CLICKHOUSE_IP, KAFKA_IP, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID, AWS_REGION, AWS_BUCKET
import clickhouse_connect
from kafka import KafkaConsumer
import logging, json, boto3
logger = logging.getLogger(__name__)

# aws connection
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
bucket_name = AWS_BUCKET

# historical market data
def market_clickhouse_client():      
    client = clickhouse_connect.get_client(
        host=MARKET_DATA_CLICKHOUSE_IP,
        port=8123,
        username="default",   
        password="mysecurepassword",
        database="default"
    )
    return client

# portfolio data
def trading_clickhouse_client():
    return clickhouse_connect.get_client(
        host="clickhouse",
        # host="localhost",
        port=8123,
        username="default",
        password="mysecurepassword",
        database="default"
    )

# real time market data
def get_kafka_data(kafka_topic, group_id):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[KAFKA_IP],
        auto_offset_reset="latest",   
        enable_auto_commit=False,   
        group_id=group_id,
    )
    return consumer

# return latest message from the consumer (this should be optimized to "hang")
def get_latest_price(consumer):
    consumer.poll(timeout_ms=100)
    try:
        for tp in consumer.assignment():
            consumer.seek_to_end(tp)
        msg = next(consumer)
        return json.loads(msg.value.decode("utf-8")).get("price")
    except Exception:
        logger.exception("Error getting latest message from Kafka consumer")
        return None
    