# setup.py

# one off container to run at the start
# docker compose run --rm db_setup

from data import trading_clickhouse_client
from strategies import get_strategies
import logging
logger = logging.getLogger(__name__)

client = trading_clickhouse_client()

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
            strategy_name String,
            strategy_description String
        ) ENGINE = ReplacingMergeTree()
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
        PRIMARY KEY (strategy_name, symbol)
    """)
    logger.info("Portfolio table time series created in ClickHouse.")


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

try:
    logger.info("Deleting tables.")
    delete_portfolio_tables(client)
    delete_execution_table(client)
except Exception as e:
    logger.warning(f"Error when deleting tables: {e}")

try:
    logger.info("Creating tables.")
    create_portfolio_table_key(client)
    create_portfolio_table_timeseries(client)
    create_execution_table(client)
except Exception as e:
    logger.warning(f"Error when creating tables: {e}")

try:
    logger.info("Initializing portfolio values.")
    for strat in get_strategies(stop_event=None):
        strat.initialize_pf()
except Exception:
    logger.warning(f"Error when initializing portfolios: {e}")