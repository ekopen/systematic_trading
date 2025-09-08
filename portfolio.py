# portfolio.py
# Portfolio management module

from data import get_latest_price
import logging, json
logger = logging.getLogger(__name__)

def delete_portfolio_table(client):

    logger.info("Deleting portfolio table if exists.")
    client.command("DROP TABLE IF EXISTS portfolio_db SYNC")
    logger.info("Portfolio table deleted in ClickHouse.")

def create_portfolio_table(client):

    logger.info("Creating portfolio table if not exists.")
    client.command("""
        CREATE TABLE IF NOT EXISTS portfolio_db (
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
    logger.info("Portfolio table created in ClickHouse.")



def initialize_portfolio(client, consumer, starting_cash, symbol, starting_market_value, strategy_names):

    logger.info("Initializing portfolio with starting cash and market value.")

    msg = get_latest_price(consumer)
    initialization_price = json.loads(msg.value.decode("utf-8")).get("price")

    init_arr =[starting_cash, symbol, starting_market_value / initialization_price, starting_market_value, strategy_names[0]]
    column_names = ["cash_balance", "symbol", "quantity", "market_value", "strategy_name"]

    client.insert("portfolio_db", [init_arr], column_names)
    
    logger.info("Portfolio initialized with starting values.")

    result = client.query_df("SELECT * FROM portfolio_db")
    print(result)

def insert_transaction(client, signal, symbol, execution_price, quantity, strategy_name):

    logger.info("Inserting transaction into portfolio.")

    if signal == "BUY":
        direction = 1
    if signal == "SELL":
        direction = -1

    # have execution evaluate limits, so will need to move this to execution.py
    #maybe break this apart, so this is automated, and all execution updates is the quantity.
    result = client.query_df(f"SELECT * FROM portfolio_db WHERE strategy_name = '{strategy_name}' ORDER BY time DESC LIMIT 1")
    last_cash_balance = result['cash_balance'].values[0]
    last_quantity = result['quantity'].values[0]
    cash_balance = last_cash_balance - (execution_price * quantity * direction)
    quantity = last_quantity + (quantity * direction)
    market_value = quantity * execution_price

    transaction_arr = [cash_balance, symbol, quantity, market_value, strategy_name]
    column_names = ["cash_balance", "symbol", "quantity", "market_value", "strategy_name"]

    client.insert("portfolio_db", [transaction_arr], column_names)
    
    logger.info(f"Inserted transaction for {strategy_name} into portfolio.")

    result = client.query_df("SELECT * FROM portfolio_db")
    print(result)
