# signal_engine.py
# Signal generation module

import logging, time
logger = logging.getLogger(__name__)

def start_signal_genetator(stop_event):
    logger.info("Signal generator started.")
    try:
        while not stop_event.is_set():
            try:
                time.sleep(5)
                logger.info("Signal generator finished processing.")
            except Exception:
                logger.exception("Error in signal generator loop")
    except Exception:
        logger.exception("Error in signal generator")
    finally:
        try:
            logger.info("Signal generator shutting down.")
        except Exception:
            logger.exception("Error during signal generator shutdown.")
        logger.info("Signal generator stopped.")
