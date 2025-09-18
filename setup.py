# setup.py

# one off container to run at the start
# docker compose run --rm db_setup

from data import trading_clickhouse_client
from portfolio import create_portfolio_table_key, create_portfolio_table_timeseries, initialize_portfolio, delete_portfolio_tables
from execution import create_execution_table, delete_execution_table
from strategies import get_strategies

client = trading_clickhouse_client()

# delete_portfolio_tables(setup_client)
# delete_execution_table(setup_client)

create_portfolio_table_key(client)
create_portfolio_table_timeseries(client)
create_execution_table(client)

for strat in get_strategies(stop_event=None): 
    print(f"Initializing {strat.strategy_name}...")
    strat.initialize_pf()