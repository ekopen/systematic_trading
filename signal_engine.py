# signal_engine.py
# Signal generation module

import logging, time, json
import clickhouse_connect
from kafka import KafkaConsumer
from execution_engine import execute_trade

logger = logging.getLogger(__name__)

def start_signal_generator(stop_event):

    logger.info("Signal generator started.")


    client = clickhouse_connect.get_client(
        host="159.65.41.22",
        port=8123,   # or 9000 if you prefer the native protocol
        username="default",   # or your custom user
        password="mysecurepassword",
        database="default"
    )

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

                # --- Get recent average from ClickHouse ---
                recent_avg_df = client.query_df("SELECT AVG(price) AS recent_avg FROM (SELECT price FROM ticks_db ORDER BY timestamp DESC LIMIT 100) sub")
                recent_avg_price = recent_avg_df.values[0][0]
                logger.info(f"Recent average price: {recent_avg_price}")

                # --- Get the latest Kafka price ---
                msg = next(consumer)
                data = json.loads(msg.value.decode("utf-8"))
                current_price = data.get("price")
                logger.info(f"Latest price: {current_price}")
                
                # --- Simple signal logic ---
                if current_price > recent_avg_price:
                    execute_trade("SELL")
                else:   
                    execute_trade("BUY")

                logger.info("Signal generator finished processing.")
                time.sleep(5)
            except Exception:
                logger.exception("Error in signal generator loop")
    except Exception:
        logger.exception("Error in signal generator")
    finally:
        try:
            logger.info("Signal generator shutting down.")
            consumer.close()
        except Exception:
            logger.exception("Error during signal generator shutdown.")
        logger.info("Signal generator stopped.")
