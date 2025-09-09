# main.py
# starts and stops the trading module

# imports
import threading, time, signal, logging
from data import market_clickhouse_client, trading_clickhouse_client, get_kafka_data, get_latest_price
from portfolio import  delete_portfolio_tables, initialize_portfolio, create_portfolio_table_key, create_portfolio_table_timeseries, portfolio_monitoring
from execution import delete_execution_table, create_execution_table
from signals import generate_signals
from config import MONITOR_FREQUENCY

from strategies import StrategyTemplate

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


test_strategy = StrategyTemplate(stop_event,
                                 kafka_topic = "price_ticks", symbol="ETH", strategy_name="MeanReversion",
                                 starting_cash=100000, starting_mv=100000, frequency=MONITOR_FREQUENCY)

test_strategy_v2 = StrategyTemplate(stop_event,
                                 kafka_topic = "price_ticks", symbol="ETH", strategy_name="MeanReversion_v2",
                                 starting_cash=10000, starting_mv=10000, frequency=MONITOR_FREQUENCY)

strat_arr = [test_strategy, test_strategy_v2]

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

        for strat in strat_arr:
            strat.initialize_pf()
            strat.run_strategy()

        while not stop_event.is_set():
             time.sleep(1)

        logger.info("System shutdown complete.") 

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        stop_event.set()
    except Exception:
        logger.exception("Fatal error in main loop")
        