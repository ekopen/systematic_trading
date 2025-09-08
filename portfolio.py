# portfolio.py
# Portfolio management module

from market_data import get_latest_price
import clickhouse_connect
import logging, json
logger = logging.getLogger(__name__)

def portfolio_client():
    return clickhouse_connect.get_client(
        host="clickhouse", 
        port=8123,
        username="default",
        password="mysecurepassword",
        database="default"
    )

def create_portfolio_table(client):

    logger.info("Creating portfolio table if not exists.")
    client.command("""
        CREATE TABLE IF NOT EXISTS portfolio_db (
            transaction_time DateTime64(3, 'UTC') DEFAULT now64(3),
            cash_balance Float64,
            symbol String,
            quantity Float64,
            price Float64,
            market_value Float64,
            strategy_name String
        ) ENGINE = MergeTree()
        ORDER BY (strategy_name, symbol, transaction_time)
        TTL transaction_time + INTERVAL 7 DAY DELETE
    """)
    logger.info("Portfolio table created in ClickHouse.")

def delete_portfolio_table(client):

    logger.info("Deleting portfolio table if exists.")
    client.command("DROP TABLE IF EXISTS portfolio_db")
    logger.info("Portfolio table deleted in ClickHouse.")

def initialize_portfolio(client, consumer, starting_cash, symbol, starting_market_value, strategy_names):

    logger.info("Initializing portfolio with starting cash and market value.")

    create_portfolio_table(client)

    msg = get_latest_price(consumer)
    initialization_price = json.loads(msg.value.decode("utf-8")).get("price")

    client.insert("portfolio_db", {
        "cash_balance": starting_cash,
        "symbol": symbol,
        "quantity": starting_market_value / initialization_price,
        "market_value": starting_market_value,
        "strategy_name": strategy_names[0]
        })
    
    logger.info("Portfolio initialized with starting values.")

    result = client.query_df("SELECT * FROM portfolio_db")
    df = result.to_pandas()
    print(df)

def insert_transaction(client, signal, symbol, execution_price, quantity, strategy_name):

    logger.info("Inserting transaction into portfolio.")

    if signal == "BUY":
        direction = 1
    if signal == "SELL":
        direction = -1

    # have execution evaluate limits, so will need to move this to execution.py
    #maybe break this apart, so this is automated, and all execution updates is the quantity.
    result = client.query_df(f"SELECT * FROM portfolio_db WHERE strategy_name = '{strategy_name}' ORDER BY transaction_time DESC LIMIT 1")
    df = result.to_pandas()
    last_cash_balance = df['cash_balance'].values[0]
    last_quantity = df['quantity'].values[0]
    cash_balance = last_cash_balance - (execution_price * quantity * direction)
    quantity = last_quantity + (quantity * direction)
    market_value = quantity * execution_price

    client.insert("portfolio_db", {
        "cash_balance": cash_balance,
        "symbol": symbol,
        "quantity": quantity,
        "market_value": market_value,
        "strategy_name": strategy_name
        })
    
    logger.info(f"Inserted transaction for {strategy_name} into portfolio.")

    result = client.query_df("SELECT * FROM portfolio_db")
    df = result.to_pandas()
    print(df)
