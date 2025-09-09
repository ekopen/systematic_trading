# strategies.py
# module that contains a default class for all strategies and the specific strategies built on it.

import threading, logging
from data import get_kafka_data, get_latest_price, market_clickhouse_client, trading_clickhouse_client
from portfolio import initialize_portfolio, portfolio_monitoring
from signals import generate_signals
logger = logging.getLogger(__name__)

class StrategyTemplate:

    def __init__(self, stop_event, kafka_topic, symbol, strategy_name, starting_cash, starting_mv, frequency):
        self.stop_event = stop_event
        self.kafka_topic = kafka_topic
        self.symbol = symbol
        self.strategy_name = strategy_name

        self.starting_cash = starting_cash
        self.starting_mv = starting_mv
        self.frequency = frequency

    def initialize_pf(self):
        logger.info(f"Initializing portfolio for {self.symbol}, {self.strategy_name}.")
        try:
            init_consumer = get_kafka_data(self.kafka_topic, f"{self.strategy_name}{self.symbol}-init")
            initialization_price = get_latest_price(init_consumer)
            trading_client = trading_clickhouse_client()
            initialize_portfolio(trading_client, self.starting_cash, self.symbol, self.starting_mv, self.strategy_name, initialization_price)
            init_consumer.close()
        except Exception:
            logger.exception(f"Error initializing portfolio for {self.symbol}, {self.strategy_name}.")

    def start_portfolio_monitoring(self):
        logger.info(f"Beginning portfolio monitoring for {self.symbol}, {self.strategy_name}.")
        try:
            consumer = get_kafka_data(self.kafka_topic, f"{self.strategy_name}{self.symbol}-monitor")
            trading_client = trading_clickhouse_client()
            portfolio_monitoring(self.stop_event, self.frequency, self.symbol, self.strategy_name, consumer, trading_client)
        except Exception:
            logger.exception(f"Error beginning portfolio monitoring for {self.symbol}, {self.strategy_name}.")

    def start_signal_engine(self):
        logger.info(f"Beginning signal generation for {self.symbol}, {self.strategy_name}.")
        try:
            consumer = get_kafka_data(self.kafka_topic, f"{self.strategy_name}{self.symbol}-signals")
            market_client = market_clickhouse_client()
            trading_client = trading_clickhouse_client()
            generate_signals(self.stop_event, market_client, consumer, trading_client, self.strategy_name, self.symbol)
        except Exception:
            logger.exception(f"Error beginning signal generation for {self.symbol}, {self.strategy_name}.")

    def run_strategy(self):
        logger.info(f"Running strategy for {self.symbol}, {self.strategy_name}.")
        try:
            t1 = threading.Thread(target=self.start_portfolio_monitoring, daemon=True)
            t2 = threading.Thread(target=self.start_signal_engine, daemon=True)
            t1.start()
            t2.start()
        except Exception:
            logger.exception(f"Error running strategy for {self.symbol}, {self.strategy_name}.")


