# execution_engine.py
# Execution engine module

import logging
logger = logging.getLogger(__name__)
from portfolio import insert_transaction


def execute_trade(signal, execution_price, qty, strategy_name, symbol):
    if signal == "BUY":
        logger.info("Placing buy order...")
        insert_transaction(execution_price, qty, signal, strategy_name, symbol)
        logger.info(f"Submitted buy order at execution price {execution_price}")

    if signal == "SELL":
        logger.info("Placing sell order...")
        insert_transaction(execution_price, qty, signal, strategy_name, symbol)
        logger.info(f"Submitted sell order at execution price {execution_price}")