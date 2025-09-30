# strategy_template.py
# module that contains a default class for all strategies

import threading, logging
from data import get_kafka_data, get_latest_price, market_clickhouse_client, trading_clickhouse_client
from portfolio import initialize_portfolio, portfolio_monitoring, get_cash_balance
from ml_functions import get_production_data_seconds, build_features, get_ml_model
from execution import execute_trade
logger = logging.getLogger(__name__)

class StrategyTemplate:

    def __init__(self, stop_event, kafka_topic, symbol, strategy_name, starting_cash, starting_mv, monitor_frequency, strategy_description, execution_frequency, s3_key, local_path):
        self.stop_event = stop_event
        self.kafka_topic = kafka_topic
        self.symbol = symbol
        self.strategy_name = strategy_name
        self.starting_cash = starting_cash
        self.starting_mv = starting_mv
        self.monitor_frequency = monitor_frequency
        self.strategy_description = strategy_description
        self.execution_frequency = execution_frequency
        self.s3_key = s3_key
        self.local_path = local_path

    def initialize_pf(self):
        logger.info(f"Initializing portfolio for {self.symbol}, {self.strategy_name}.")
        try:
            trading_client = trading_clickhouse_client()
            init_consumer = get_kafka_data(self.kafka_topic, f"{self.strategy_name}{self.symbol}-init")
            initialization_price = get_latest_price(init_consumer)
            initialize_portfolio(trading_client, self.starting_cash, self.symbol, self.starting_mv, self.strategy_name, initialization_price, self.strategy_description)
            init_consumer.close()
        except Exception as e:
            logger.exception(f"Error initializing portfolio for {self.symbol}, {self.strategy_name}: {e}")

    def start_portfolio_monitoring(self):
        logger.info(f"Beginning portfolio monitoring for {self.symbol}, {self.strategy_name}.")
        try:
            consumer = get_kafka_data(self.kafka_topic, f"{self.strategy_name}{self.symbol}-monitor")
            trading_client = trading_clickhouse_client()
            portfolio_monitoring(self.stop_event, self.monitor_frequency, self.symbol, self.strategy_name, consumer, trading_client)
        except Exception as e:
            logger.exception(f"Error beginning portfolio monitoring for {self.symbol}, {self.strategy_name}: {e}")

    def ml_strategy(self, market_client, consumer, trading_data_client):
        logger.info(f"Beginning strategy signal generation for {self.symbol}, {self.strategy_name}.")
        try:
            # SET TRADE FREQUENCY
            if self.stop_event.wait(self.execution_frequency): # 15 seconds default
                return None, None, None, None

            # GET PRODUCTION DATA
            prod_df = get_production_data_seconds(market_client)
            feature_df = build_features(prod_df)
            feature_df_clean = feature_df.drop(columns=["price", "ts"]).iloc[[-1]]

            # GET MODEL
            ml_model = get_ml_model(self.s3_key, self.local_path)

            # PREDICT FROM MODEL
            prediction = ml_model.predict(feature_df_clean)[0]
            if prediction == 2:
                decision = "BUY"
            elif prediction == 0:
                decision = "SELL"
            else:
                decision = "HOLD"

            # DETERMINE TRADE SIZE
            current_price = get_latest_price(consumer)
            cash_balance = get_cash_balance(trading_data_client, self.strategy_name, self.symbol)
            allocation_pct = 0.25
            qty = (cash_balance * allocation_pct) / current_price if decision in ["BUY", "SELL"] else 0

            # RECORD EXECUTION LOGIC
            execution_logic = (
                f"{self.strategy_name} decision: {decision}\n"
                f"Features: {feature_df_clean.to_dict(orient='records')[0]}\n"
                f"Current price: {current_price:.2f}"
            )

            #SEND TRADE TO EXECTUION ENGINE
            execute_trade(trading_data_client, consumer, decision, current_price, qty, self.strategy_name, self.symbol, execution_logic)

        except Exception as e:
            logger.exception(f"Error in {self.strategy_name} strategy: {e}")

    def run_strategy(self):
        logger.info(f"Running strategy for {self.symbol}, {self.strategy_name}.")
        try:
            market_client = market_clickhouse_client()
            consumer = get_kafka_data(self.kafka_topic, f"{self.strategy_name}{self.symbol}-signals")
            trading_client = trading_clickhouse_client()
            t1 = threading.Thread(target=self.start_portfolio_monitoring, daemon=True)
            t2 = threading.Thread(target=self.ml_strategy, args=(market_client, consumer, trading_client), daemon=True)
            t1.start()
            t2.start()
            return [t1, t2]
        except Exception as e:
            logger.exception(f"Error running strategy for {self.symbol}, {self.strategy_name}: {e}")
            return []