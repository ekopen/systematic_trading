# setup.py

# one off container to run when resetting strategies
# docker compose run --rm db_setup

from data import trading_clickhouse_client
from strategies import get_strategies
import logging
logger = logging.getLogger(__name__)

client = trading_clickhouse_client()

try:
    logger.info("Initializing portfolio values.")
    for strat in get_strategies(stop_event=None):
        strat.initialize_pf()
except Exception as e:
    logger.warning(f"Error when initializing portfolios: {e}")