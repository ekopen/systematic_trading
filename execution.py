# execution_engine.py
# Execution engine module

import logging
logger = logging.getLogger(__name__)
from portfolio import portfolio_key_order_update

def delete_execution_table(client):
    logger.info("Deleting execution table if exists.")
    client.command("DROP TABLE IF EXISTS execution_db SYNC")
    logger.info("Execution table deleted in ClickHouse.")

def create_execution_table(client):
    logger.info("Creating execution table if not exists.")
    client.command("""
        CREATE TABLE IF NOT EXISTS execution_db (
            time DateTime DEFAULT now(),
            symbol String,
            quantity Float64,
            model_price Float64,
            executed_price Float64,
            strategy_name String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol, time)
        TTL time + INTERVAL 1 DAY
    """)
    logger.info("Execution table created in ClickHouse.")

def update_execution(client, symbol, quantity, model_price, execution_price, strategy_name):
    logger.info("Updating execution record in execution table.")
    arr = [symbol, quantity, model_price, execution_price, strategy_name]
    column_names = ["symbol", "quantity", "model_price","executed_price", "strategy_name"]
    client.insert("execution_db", [arr], column_names)
    logger.info("Execution record updated in execution table.")

def execute_trade(client, signal, model_price, execution_price, qty, strategy_name, symbol):

    if signal == "BUY":
        direction = 1
    if signal == "SELL":
        direction = -1

    logger.info(f"Placing an order with the following parameters: Signal: {signal}, {model_price}, {execution_price}, {qty}, {strategy_name}, {symbol}")
    update_execution(client, symbol, qty * direction, model_price, execution_price, strategy_name)
    portfolio_key_order_update(client, symbol, qty * direction, qty * direction * execution_price , strategy_name)