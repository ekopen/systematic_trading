# execution_engine.py
# Execution engine module

import logging
logger = logging.getLogger(__name__)

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from config import API_KEY, API_SECRET, BASE_URL

api_key = API_KEY
api_secret = API_SECRET
trading_client = TradingClient(api_key, api_secret, paper=True)

def execute_trade(signal):
    if signal == "BUY":
        logger.info("Placing buy order...")
        # Prepare the buy order data
        buy_order_data = MarketOrderRequest(
            symbol="ETH/USD",
            qty=1,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC
        )
        # Submit the order to Alpaca
        buy_order = trading_client.submit_order(order_data=buy_order_data)
        logger.info(f"Submitted buy order for {buy_order.qty} shares of {buy_order.symbol}")

    else:
        logger.info("placing sell order...")
        # Prepare the sell order data
        sell_order_data = MarketOrderRequest(
            symbol="ETH/USD",
            qty=1,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC
        )
        # Submit the order to Alpaca
        sell_order = trading_client.submit_order(order_data=sell_order_data)
        logger.info(f"Submitted sell order for {sell_order.qty} shares of {sell_order.symbol}")

    # Get a list of all your current positions
    portfolio = trading_client.get_all_positions()

    # Print the quantity of shares for each position
    for position in portfolio:
        print(f"{position.qty} shares of {position.symbol}")