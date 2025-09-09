# main.py
# starts and stops the trading module

# imports
import threading, time, signal, logging
from data import market_clickhouse_client, trading_clickhouse_client, get_kafka_data, get_latest_price
from portfolio import  delete_portfolio_tables, initialize_portfolio, create_portfolio_table_key, create_portfolio_table_timeseries, portfolio_monitoring
from execution import delete_execution_table, create_execution_table
from signals import generate_signals
from config import MONITOR_FREQUENCY

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
symbol = "ETH"
strategy_name = "MeanReversion"

# logic for portfolio monitoring and signal engine threads, which need their own clients and consumers
def start_portfolio_monitoring(stop_event, frequency, symbol, strategy_name, kafka_topic):
    client = trading_clickhouse_client()
    consumer = get_kafka_data(kafka_topic)
    portfolio_monitoring(stop_event, frequency, symbol, strategy_name, consumer, client)
def start_signal_engine(stop_event, kafka_topic):
    market_client = market_clickhouse_client()
    trading_client = trading_clickhouse_client()
    consumer = get_kafka_data(kafka_topic)
    generate_signals(stop_event, market_client, consumer, trading_client)

# start/stop loop
if __name__ == "__main__":
    try:
        logger.info("System starting.")

        # setup portfolio and execution tables
        setup_client = trading_clickhouse_client()
        delete_portfolio_tables(setup_client)
        create_portfolio_table_key(setup_client)
        create_portfolio_table_timeseries(setup_client)
        delete_execution_table(setup_client)
        create_execution_table(setup_client)

        # initialize starting portfolio
        init_consumer = get_kafka_data(kafka_topic)
        initialization_price = get_latest_price(init_consumer)
        initialize_portfolio(setup_client, starting_cash=10000, symbol=symbol, starting_market_value=100000, strategy_names=[strategy_name], initialization_price=initialization_price)
        init_consumer.close()

        # start frequent portfolio monitoring and signal generation via threads
        t1 = threading.Thread(target=start_portfolio_monitoring, args=(stop_event, MONITOR_FREQUENCY, symbol, strategy_name, kafka_topic))
        t2 = threading.Thread(target=start_signal_engine, args=(stop_event, kafka_topic))
        t1.start()
        t2.start()

        while not stop_event.is_set():
             time.sleep(1)

        logger.info("System shutdown complete.") 

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        stop_event.set()
    except Exception as e:
        logger.exception("Fatal error in main loop")
        