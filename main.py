# main.py
# starts and stops the data pipeline

# imports
import threading, time, signal, logging
from kafka_consumer import start_consumer

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

# shutdown
stop_event = threading.Event()
def handle_signal(signum, frame):
    logger.info("Received stop signal. Shutting down...")
    stop_event.set()
signal.signal(signal.SIGTERM, handle_signal)

# start/stop loop
if __name__ == "__main__":
    try:
        logger.info("System starting.")

        consumer_thread = threading.Thread(target=start_consumer, args=(stop_event,))
        consumer_thread.start()

        while not stop_event.is_set():
             time.sleep(1)

        consumer_thread.join(timeout=3)
        logger.info("System shutdown complete.")

    except Exception as e:
        logger.exception("Fatal error in main loop")
        