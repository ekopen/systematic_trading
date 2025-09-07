# main.py
# starts and stops the trading module

# imports
import threading, time, signal, logging
from signal_engine import start_signal_generator

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
signal.signal(signal.SIGINT, handle_signal) # CTRL+C shutdown

# start/stop loop
if __name__ == "__main__":
    try:
        logger.info("System starting.")

        signal_engine_thread = threading.Thread(target=start_signal_generator, args=(stop_event,))
        signal_engine_thread.start()

        while not stop_event.is_set():
             time.sleep(1)

        logger.info("System shutdown complete.") 

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        stop_event.set()
    except Exception as e:
        logger.exception("Fatal error in main loop")
        