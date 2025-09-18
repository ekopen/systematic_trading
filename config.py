# config.py
# variables that are used across the project

MONITOR_FREQUENCY = 30 # seconds per monitoring instance, where we record portfolio data

MAX_DRAWDOWN = .5 #maximum amount of portfolio value willing to lose before pausing trading
MAX_ALLOCATION = .25 #maxmimum amount of value per trade compared to portfolio value willing to make

market_data_clickhouse_ip = "159.203.124.175"
kafka_ip = "159.65.41.22:9092"



