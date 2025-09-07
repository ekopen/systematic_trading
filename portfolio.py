# portfolio.py
# Portfolio management module

from alpaca.trading.client import TradingClient
from config import API_KEY, API_SECRET
api_key = API_KEY
api_secret = API_SECRET
trading_client = TradingClient(api_key, api_secret, paper=True)

account = trading_client.get_account()
positions = trading_client.get_all_positions()

#print(account.cash)
print(positions)
