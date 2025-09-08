# portfolio.py
# Portfolio management module

from data import get_latest_price
import logging, json, time
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
            cash_balance Float64,
            symbol String,
            quantity Float64,
            market_value Float64,
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
            strategy_name String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol, time)
        TTL time + INTERVAL 1 DAY
    """)
    logger.info("Portfolio table time series created in ClickHouse.")

def initialize_portfolio(client, starting_cash, symbol, starting_market_value, strategy_names, initialization_price):
    logger.info("Initializing portfolio with starting cash and market value.")
    init_arr = [
        starting_cash,
        symbol,
        starting_market_value / initialization_price,
        starting_market_value,
        strategy_names[0],
    ]
    column_names = ["cash_balance", "symbol", "quantity", "market_value", "strategy_name"]
    client.insert("portfolio_db_key", [init_arr], column_names)
    logger.info("Portfolio initialized with starting values.")

def portfolio_key_order_update(client, symbol, quantity_change, market_value_change, strategy_name):
    logger.info("Updating portfolio key table with a new order.")
    client.command(f"""
        ALTER TABLE portfolio_db_key UPDATE quantity = quantity + {quantity_change}, market_value = market_value + {market_value_change}, cash_balance = cash_balance + {-market_value_change}
        WHERE symbol = '{symbol}' AND strategy_name = '{strategy_name}'
    """)
    logger.info("Portfolio key table updated with a new order.")

def portfolio_monitoring(stop_event, frequency, consumer, client):
    logger.info("Starting portfolio monitoring thread.")
    try:
        time.sleep(frequency)
        while not stop_event.is_set():
            # periodic monitoring and updating of portfolio values
            logger.info("Updating portfolio key table per regular monitoring.")

            #CURRENTLY HARD CORDED
            symbol = "ETH"
            strategy_name = "MeanReversion"

            msg = get_latest_price(consumer)
            price = json.loads(msg.value.decode("utf-8")).get("price")

            client.command(f"""
                ALTER TABLE portfolio_db_key UPDATE  market_value = quantity * {price}
                WHERE symbol = '{symbol}' AND strategy_name = '{strategy_name}'
            """)

            logger.info("Updated portfolio key table per regular monitoring.")

            # insert into timeseries table

            logger.info("Inserting latest portfolio key values into timeseries table.")

            rows = client.query(f"""
                SELECT *
                FROM portfolio_db_key
                WHERE symbol = '{symbol}' AND strategy_name = '{strategy_name}'
            """).result_rows

            client.insert(
                "portfolio_db_ts",
                rows,
                column_names=["cash_balance", "symbol", "quantity", "market_value", "strategy_name"]
                )
            
            rows_2 = client.query(f"""
                SELECT *
                FROM portfolio_db_ts
            """).result_rows

            
            logger.info("Inserted latest portfolio key values into timeseries table.")

            print(f"Current key table values: {rows}")
            print(f"Current time series table values: {rows_2}")

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