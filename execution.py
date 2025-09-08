# execution_engine.py
# Execution engine module

import logging
logger = logging.getLogger(__name__)
from portfolio import insert_transaction

def celte_execution_table(client):

    logger.info("Deleting portfolio table if exists.")
    client.command("DROP TABLE IF EXISTS portfolio_db SYNC")
    logger.info("Portfolio table deleted in ClickHouse.")

def create_execution_table(client):
    # build in TTL eventually
    logger.info("Creating execution table if not exists.")
    client.command("""
        CREATE TABLE IF NOT EXISTS execution_db (
            time DateTime DEFAULT now(),
            cash_balance Float64,
            symbol String,
            quantity Float64,
            market_value Float64,
            strategy_name String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol, transaction_time)
        TTL time + INTERVAL 1 DAY
    """)
    logger.info("Portfolio table created in ClickHouse.")



def execute_trade(client, signal, execution_price, qty, strategy_name, symbol):

    if signal == "BUY":
        logger.info("Placing buy order...")
        insert_transaction(client, signal, symbol, execution_price, qty, strategy_name)
        logger.info(f"Submitted buy order at execution price {execution_price}")

    if signal == "SELL":
        logger.info("Placing sell order...")
        insert_transaction(client, signal, symbol, execution_price, qty, strategy_name)
        logger.info(f"Submitted sell order at execution price {execution_price}")