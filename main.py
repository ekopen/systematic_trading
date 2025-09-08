# main.py
# starts and stops the trading module

# imports
import threading, time, signal, logging
from data import market_clickhouse_client, trading_clickhouse_client, get_kafka_data
from portfolio import  initialize_portfolio, delete_portfolio_table, create_portfolio_table
from signals import generate_signals

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

# subscribing to a specific kafka topic
kafka_topic = "price_ticks"

# start/stop loop
if __name__ == "__main__":
    try:
        logger.info("System starting.")

        # get data necessary for the signal engine and begin the thread
        trading_data_client = trading_clickhouse_client()
        market_data_client = market_clickhouse_client()
        kafka_stream = get_kafka_data(kafka_topic)

        delete_portfolio_table(trading_data_client)
        create_portfolio_table(trading_data_client)
        initialize_portfolio(trading_data_client, kafka_stream, starting_cash=100000, symbol="ETH", starting_market_value=100000, strategy_names=["MeanReversion"])

        # start signal engine thread
        signal_engine_thread = threading.Thread(target=generate_signals, args=(stop_event, market_data_client, kafka_stream, trading_data_client))
        signal_engine_thread.start()

        while not stop_event.is_set():
             time.sleep(1)

        logger.info("System shutdown complete.") 

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        stop_event.set()
    except Exception as e:
        logger.exception("Fatal error in main loop")
        