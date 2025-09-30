# config.py
# variables that are used across the project
import os
from dotenv import load_dotenv
load_dotenv() # for when running locally

MODEL_REFRESH_INTERVAL = 43200 # how often to refresh ml models, in seconds

MAX_DRAWDOWN = .5 #maximum amount of portfolio value willing to lose before pausing trading
MAX_ALLOCATION = .5 #maxmimum amount of value per trade compared to portfolio value willing to make
MAX_SHORT = 200000 #max short value of any portfolio

MARKET_DATA_CLICKHOUSE_IP = "159.203.124.175"
KAFKA_IP = "159.65.41.22:9092"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")


