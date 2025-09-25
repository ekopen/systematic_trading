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
from portfolio import get_cash_balance
from data import get_latest_price
rf_model = joblib.load("models/random_forest_-_second_features.pkl")

# this is found in the ml module
def build_features(df):
    # standard features
    df["returns"] = df["price"].pct_change()
    df["sma_30"] = df["price"].rolling(window=30).mean()
    df["sma_60"] = df["price"].rolling(window=60).mean()
    df["momentum_10"] = df["price"] / df["price"].shift(10) - 1
    df["momentum_30"] = df["price"] / df["price"].shift(30) - 1
    df["volatility_30"] = df["returns"].rolling(window=30).std()
    for lag in [1, 2, 5]:
        df[f"lag_return_{lag}"] = df["returns"].shift(lag)

    # RSI
    delta = df["price"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["14"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = df["price"].ewm(span=12, adjust=False).mean()
    ema26 = df["price"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    
    df = df.dropna()
    return df


def ml_random_forest(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    ML-driven trading strategy using RandomForest predictions.
    Features: returns, SMA20, SMA60, volatility.
    Labels: 0 = SELL, 1 = HOLD, 2 = BUY
    """
    try:
        if stop_event.wait(15):
            return None, None, None, None

        # Get recent historical data (same as feature engineering during training)
        # 120 rows to get all features populated
        df = market_data_client.query_df("""
            SELECT 
                toStartOfInterval(timestamp, INTERVAL 15 SECOND) AS ts, 
                avg(price) AS price
            FROM ticks_db
            GROUP BY ts
            ORDER BY ts DESC
            LIMIT 120
        """)
        df = df.sort_index()
        df = build_features(df)
        latest_features = df.drop(columns=["price", "ts"]).iloc[-1].values.reshape(1, -1)

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