# strategies.py
# contains all strategy functions, and packages them so they can be sent to main.py


import logging
from data import get_latest_price
from strategy_template import StrategyTemplate
from portfolio import get_cash_balance
from config import MONITOR_FREQUENCY
logger = logging.getLogger(__name__)

#ml imports
import joblib
import numpy as np
import pandas as pd
from portfolio import get_cash_balance
from data import get_latest_price
rf_model = joblib.load("models/rf_model.pkl")


def ml_random_forest(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    ML-driven trading strategy using RandomForest predictions.
    Features: returns, SMA20, SMA60, volatility.
    Labels: 0 = SELL, 1 = HOLD, 2 = BUY
    """
    try:
        if stop_event.wait(60):
            return None, None, None, None

        # Get recent historical data (same as feature engineering during training)
        df = market_data_client.query_df("""
            SELECT avg(price) AS price
            FROM ticks_db
            WHERE timestamp > now() - INTERVAL 2 HOUR
            GROUP BY toStartOfMinute(timestamp)
            ORDER BY toStartOfMinute(timestamp) DESC
            LIMIT 120
        """)
        df = df.sort_index()

        # Build features
        df["returns"] = df["price"].pct_change()
        df["sma_20"] = df["price"].rolling(20).mean()
        df["sma_60"] = df["price"].rolling(60).mean()
        df["volatility"] = df["returns"].rolling(30).std()
        df = df.dropna()

        latest_features = df[["returns", "sma_20", "sma_60", "volatility"]].iloc[-1].values.reshape(1, -1)

        # Model prediction
        prediction = rf_model.predict(latest_features)[0]

        if prediction == 2:
            decision = "BUY"
        elif prediction == 0:
            decision = "SELL"
        else:
            decision = "HOLD"

        # Recent live price
        current_price = get_latest_price(consumer)

        # Position sizing
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price if decision in ["BUY", "SELL"] else 0

        execution_logic = (
            f"ML RandomForest decision: {decision}\n"
            f"Features: {latest_features.tolist()}\n"
            f"Current price: {current_price:.2f}"
        )

        return decision, current_price, qty, execution_logic

    except Exception:
        logger.exception(f"Error in ML RandomForest strategy for {strategy_name}, {symbol}.")
        return "HOLD", 0, 0, "Error"


def long_only(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    Buy-and-hold benchmark strategy:
    - Holds the starting portfolio without trading
    - Useful as a passive performance baseline
    """
    try:
        # FREQUENCY LOGIC
        if stop_event.wait(60):
            return None, None, None, None
        
        # HISTORICAL DATA

        # RECENT DATA
        current_price = get_latest_price(consumer)

        #SIGNAL LOGIC
        decision = "HOLD"

        # SIZING LOGIC
        qty = 0

        # EXECUTION LOGIC
        execution_logic = (
            f"{decision} signal has been generated.\n"
        )

        return decision, current_price, qty, execution_logic
    except Exception:
        logger.exception(f"Error in signal generation logic for {strategy_name}, {symbol}.")
        return "HOLD", 0, 0, "Error"


def get_strategies(stop_event):
    return [
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Long_Only",
            starting_cash=0,
            starting_mv=200000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=long_only,
            strategy_description=(
                f"Holds starting portfolio without trading. "
                f"Serves as a passive benchmark."
            )
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="ML_RandomForest",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=ml_random_forest,
            strategy_description=(
                "Machine learning strategy using RandomForest classifier. "
                "Features: returns, SMA20, SMA60, volatility. "
                "Labels future returns as Buy/Sell/Hold. Runs every minute."
            )
)
    ]