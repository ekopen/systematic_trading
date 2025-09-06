import logging, threading, signal
from kafka import KafkaConsumer

# logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("log_data/app.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting file!")

# shutdown
stop_event = threading.Event()
def handle_signal(signum, frame):
    logger.info("Received stop signal. Shutting down...")
    stop_event.set()
signal.signal(signal.SIGTERM, handle_signal)


if __name__ == "__main__":
    try:
        logger.info("Starting Kafka consumer...")
        consumer = KafkaConsumer(
            "your_topic",
            bootstrap_servers=["159.65.41.22:9092"],
            auto_offset_reset="earliest",
            group_id="test-group"
        )

        logger.info("Kafka consumer started. Listening for messages...")
        for msg in consumer:
            logger.info(msg.value)

    except Exception as e:
        logger.exception("Fatal error in main loop")
        