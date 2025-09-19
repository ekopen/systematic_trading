# setup.py

# one off container to run at the start
# docker compose run --rm db_setup

from data import trading_clickhouse_client
from portfolio import create_portfolio_table_key, create_portfolio_table_timeseries, initialize_portfolio, delete_portfolio_tables
from execution import create_execution_table, delete_execution_table
from strategies import get_strategies
import logging
logger = logging.getLogger(__name__)

client = trading_clickhouse_client()

try:
    logger.info("Deleting tables.")
    delete_portfolio_tables(client)
    delete_execution_table(client)
except Exception:
    logger.warning("Error when deleting tables.")

try:
    logger.info("Creating tables.")
    create_portfolio_table_key(client)
    create_portfolio_table_timeseries(client)
    create_execution_table(client)
except Exception:
    logger.warning("Error when creating tables.")

try:
    logger.info("Starting strategies.")
    for strat in get_strategies(stop_event=None):
        strat.initialize_pf()
except Exception:
    logger.warning("Error when initializing portfolios.")