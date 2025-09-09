#strategies.py
import time, logging
from data import get_latest_price
from strategy_template import StrategyTemplate
from config import MONITOR_FREQUENCY
logger = logging.getLogger(__name__)


def mean_reversion_v1(market_data_client, consumer):
    time.sleep(60)
    recent_avg_df = market_data_client.query_df(
        "SELECT AVG(price) AS recent_avg "
        "FROM (SELECT price FROM ticks_db ORDER BY timestamp DESC LIMIT 1000) sub"
    )
    recent_avg_price = recent_avg_df.values[0][0]
    model_price = get_latest_price(consumer)
    qty = 1
    if model_price > recent_avg_price:
        decision = "SELL"
    elif model_price < recent_avg_price:
        decision = "BUY"
    else:
        decision = "HOLD"
    return decision, model_price, qty

def mean_reversion_v2(market_data_client, consumer):
    time.sleep(60)
    recent_avg_df = market_data_client.query_df(
        "SELECT AVG(price) AS recent_avg "
        "FROM (SELECT price FROM ticks_db ORDER BY timestamp DESC LIMIT 5000) sub"
    )
    recent_avg_price = recent_avg_df.values[0][0]
    model_price = get_latest_price(consumer)
    qty = 1
    if model_price > recent_avg_price:
        decision = "SELL"
    elif model_price < recent_avg_price:
        decision = "BUY"
    else:
        decision = "HOLD"
    return decision, model_price, qty

def mean_reversion_v3(market_data_client, consumer):
    time.sleep(60)
    recent_avg_df = market_data_client.query_df(
        "SELECT AVG(price) AS recent_avg "
        "FROM (SELECT price FROM ticks_db ORDER BY timestamp DESC LIMIT 10000) sub"
    )
    recent_avg_price = recent_avg_df.values[0][0]
    model_price = get_latest_price(consumer)
    qty = 1
    if model_price > recent_avg_price:
        decision = "SELL"
    elif model_price < recent_avg_price:
        decision = "BUY"
    else:
        decision = "HOLD"
    return decision, model_price, qty


def get_strategies(stop_event):
    return [
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="MeanReversion_v1",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=mean_reversion_v1,
            strategy_description="Every 60 seconds, look at the last 1000 ticks, and sell if overbought and buy if oversold."
            ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="MeanReversion_v2",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=mean_reversion_v2,
            strategy_description="Every 60 seconds, look at the last 5000 ticks, and sell if overbought and buy if oversold."
            ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="MeanReversion_v3",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=mean_reversion_v3,
            strategy_description="Every 60 seconds, look at the last 10000 ticks, and sell if overbought and buy if oversold."
            )
        ]