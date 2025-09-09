# portfolio.py
# Portfolio management module, used to manage portfolio data, update portfolio based on executed trades, and monitor portfolio performance

from data import get_latest_price
import logging, time
logger = logging.getLogger(__name__)

def delete_portfolio_tables(client):
    logger.info("Deleting portfolio tables if exists.")
    client.command("DROP TABLE IF EXISTS portfolio_db_key SYNC")
    client.command("DROP TABLE IF EXISTS portfolio_db_ts SYNC")
    logger.info("Portfolio tables deleted in ClickHouse.")

def create_portfolio_table_key(client):
    logger.info("Creating portfolio table key if not exists.")
    client.command("""
        CREATE TABLE IF NOT EXISTS portfolio_db_key (
            last_updated DateTime DEFAULT now(),
            cash_balance Float64,
            symbol String,
            quantity Float64,
            market_value Float64,
            portfolio_value Float64,
            strategy_name String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol)
    """)
    logger.info("Portfolio table key created in ClickHouse.")

def create_portfolio_table_timeseries(client):
    logger.info("Creating portfolio table time series if not exists.")
    client.command("""
        CREATE TABLE IF NOT EXISTS portfolio_db_ts (
            time DateTime DEFAULT now(),
            cash_balance Float64,
            symbol String,
            quantity Float64,
            market_value Float64,
            portfolio_value Float64,
            strategy_name String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol, time)
        TTL time + INTERVAL 1 DAY
    """)
    logger.info("Portfolio table time series created in ClickHouse.")

def initialize_portfolio(client, starting_cash, symbol, starting_market_value, strategy_name, initialization_price):
    logger.info("Initializing portfolio with starting cash and market value.")
    try:
        init_arr = [
            starting_cash,
            symbol,
            starting_market_value / initialization_price,
            starting_market_value,
            starting_cash + starting_market_value,
            strategy_name,
        ]
        column_names = ["cash_balance", "symbol", "quantity", "market_value", "portfolio_value", "strategy_name"]
        client.insert("portfolio_db_key", [init_arr], column_names)
        logger.info("Portfolio initialized with starting values.")
    except Exception:
        logger.exception("Error initializing portfolio in ClickHouse.")

def portfolio_key_order_update(client, symbol, quantity_change, market_value_change, strategy_name):
    logger.info("Updating portfolio key table with a new order.")
    try:
        client.command(f"""
            ALTER TABLE portfolio_db_key UPDATE quantity = quantity + {quantity_change}, market_value = market_value + {market_value_change}, cash_balance = cash_balance + {-market_value_change}, portfolio_value = cash_balance + market_value
            WHERE symbol = '{symbol}' AND strategy_name = '{strategy_name}'
        """)
        logger.info("Portfolio key table updated with a new order.")
    except Exception:
        logger.exception("Error updating portfolio key table in ClickHouse.")

def portfolio_monitoring(stop_event, frequency, symbol, strategy_name, consumer, client):
    logger.info("Starting portfolio monitoring thread.")
    try:
        while not stop_event.is_set():
            # periodically update market value and portfolio value based on latest price of traded symbol
            logger.info("Updating portfolio key table per regular monitoring.")
            price = get_latest_price(consumer)
            client.command(f"""
                ALTER TABLE portfolio_db_key UPDATE  market_value = quantity * {price}, portfolio_value = cash_balance + market_value
                WHERE symbol = '{symbol}' AND strategy_name = '{strategy_name}'
            """)
            logger.info("Updated portfolio key table per regular monitoring.")

            # after each update, insert latest values into timeseries table
            logger.info("Inserting latest portfolio key values into timeseries table.")
            rows = client.query(f"""
                SELECT cash_balance, symbol, quantity, market_value, strategy_name, portfolio_value
                FROM portfolio_db_key
                WHERE symbol = '{symbol}' AND strategy_name = '{strategy_name}'
            """).result_rows
            client.insert(
                "portfolio_db_ts",
                rows,
                column_names=["cash_balance", "symbol", "quantity", "market_value", "strategy_name", "portfolio_value"]
                )
            logger.info("Inserted latest portfolio key values into timeseries table.")

            # wait for next update
            logger.info(f"Sleeping for {frequency} seconds before next portfolio monitoring update.")
            time.sleep(frequency)
    except Exception:
        logger.exception("Error during portfolio monitoring loop.")

    finally:
        try:
            logger.info("Portfolio monitoring shutting down.")
            consumer.close()
        except Exception:
            logger.exception("Error during portfolio monitoring shutdown.")