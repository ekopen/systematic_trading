# execution.py
# Execution engine module, used to execute trades based on signals from the signal engine within parameters set by the risk management module

import logging, time
logger = logging.getLogger(__name__)
from portfolio import portfolio_key_order_update
from data import get_latest_price
from risk import run_risk_checks

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
            execution_logic String,
            quantity Float64,
            model_price Float64,
            executed_price Nullable(Float64),
            strategy_name String,
            approval_status String,
            approval_comment String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol, time)
        TTL time + INTERVAL 1 DAY
    """)
    logger.info("Execution table created in ClickHouse.")

def update_execution(client, symbol, execution_logic, quantity, model_price, execution_price, strategy_name, approval_status, approval_comment):
    logger.info("Updating execution record in execution table.")
    try:
        arr = [symbol, execution_logic, quantity, model_price, execution_price, strategy_name, approval_status, approval_comment]
        column_names = ["symbol", "execution_logic", "quantity", "model_price","executed_price", "strategy_name", "approval_status", "approval_comment"]
        client.insert("execution_db", [arr], column_names)
        logger.info("Execution record updated in execution table.")
    except Exception:
        logger.exception("Error inserting updating execution records")

def execute_trade(client, consumer, signal, model_price, qty, strategy_name, symbol, execution_logic):
    logger.info("Executing trade based on signal.")
    try:
        if signal == "BUY":
            direction = 1
        elif signal == "SELL":
            direction = -1
        else:
            direction = 0

        # check risks of trade
        passed, failures = run_risk_checks(client, symbol, qty, direction, model_price, strategy_name)
        if passed:
            approval_status = "Y"
            approval_comment = "Approved"
        else:
            approval_status = "N"
            approval_comment = "; ".join(failures)

        if approval_status == "Y":
            # in a real system, this is where the order would be routed to the exchange/broker. to simulate real conditions, we just wait a second and get the latest price again
            time.sleep(1)
            execution_price = get_latest_price(consumer)
            portfolio_key_order_update(client, symbol, qty * direction, qty * direction * execution_price , strategy_name)
            update_execution(client, symbol, execution_logic, qty * direction, model_price, execution_price, strategy_name, approval_status, approval_comment)
            logger.info(f"Trade executed and recorded. Signal: {signal}, Model Price: {model_price}, Execution Price: {execution_price}, Quantity: {qty}, Strategy: {strategy_name}, Symbol: {symbol}")
        if approval_status == "N":
            update_execution(client, symbol, execution_logic, qty * direction, model_price, None, strategy_name, approval_status, approval_comment)
            logger.warning(f"Trade not approved. Signal: {signal}, Model Price: {model_price}, Quantity: {qty}, Strategy: {strategy_name}, Symbol: {symbol}")
            
    except Exception:
        logger.exception("Error executing trade")