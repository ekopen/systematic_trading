# strategies.py
# contains all strategy functions, and packages them so they can be sent to main.py

import logging
from data import get_latest_price
from strategy_template import StrategyTemplate
from portfolio import get_cash_balance
from config import MONITOR_FREQUENCY
logger = logging.getLogger(__name__)

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

def mean_reversion(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    Mean reversion strategy:
    - Calculates 30-minute rolling mean and standard deviation
    - Buys when price is >2 std dev below mean (oversold)
    - Sells when price is >2 std dev above mean (overbought)
    - Allocates 10% of available cash per trade
    """
    try:
        # FREQUENCY LOGIC
        if stop_event.wait(60):
            return None, None, None, None
        
        # HISTORICAL DATA
        df = market_data_client.query_df("""
            SELECT avg(price) AS bar_price
            FROM ticks_db
            WHERE timestamp > now() - INTERVAL 30 MINUTE
            GROUP BY toStartOfMinute(timestamp)
            ORDER BY toStartOfMinute(timestamp) DESC
            LIMIT 30
        """)
        prices = df['bar_price'].values
        recent_avg_price = prices.mean()
        std_dev = prices.std()

        # RECENT DATA
        current_price = get_latest_price(consumer)

        #SIGNAL LOGIC
        z_score = (current_price - recent_avg_price) / std_dev
        if z_score > 2:
            decision = "SELL"
        elif z_score < -2:
            decision = "BUY"
        else:
            decision = "HOLD"

        # SIZING LOGIC
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price

        # EXECUTION LOGIC
        execution_logic = (
            f"{decision} signal has been generated.\n"
            f"Average price over 30 min: {recent_avg_price:.2f} "
            f"Standard deviation: {std_dev:.2f}.\n"
            f"Current price: {current_price:.2f}, z-score: {z_score:.2f}.\n"
        )

        return decision, current_price, qty, execution_logic
    except Exception:
        logger.exception(f"Error in signal generation logic for {strategy_name}, {symbol}.")
        return "HOLD", 0, 0, "Error"
    

def breakout_strategy(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    Breakout strategy:
    - Calculates recent 1-hour high and low
    - Buys when price breaks above recent high
    - Sells when price breaks below recent low
    - Allocates 10% of available cash per trade
    """
    try:
        if stop_event.wait(60):
            return None, None, None, None

        # HISTORICAL DATA
        df = market_data_client.query_df("""
            SELECT avg(price) AS bar_price
            FROM ticks_db
            WHERE timestamp > now() - INTERVAL 1 HOUR
            GROUP BY toStartOfMinute(timestamp)
            ORDER BY toStartOfMinute(timestamp) DESC
            LIMIT 60
        """)
        prices = df['bar_price'].values
        recent_high = prices.max()
        recent_low = prices.min()

        # RECENT DATA
        current_price = get_latest_price(consumer)

        # SIGNAL LOGIC
        if current_price > recent_high:
            decision = "BUY"
        elif current_price < recent_low:
            decision = "SELL"
        else:
            decision = "HOLD"

        # SIZING LOGIC
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price

        # EXECUTION LOGIC
        execution_logic = (
            f"{decision} signal generated.\n"
            f"Recent High: {recent_high:.2f}, Recent Low: {recent_low:.2f}\n"
            f"Current price: {current_price:.2f}."
        )

        return decision, current_price, qty, execution_logic
    except Exception:
        logger.exception(f"Error in breakout strategy for {strategy_name}, {symbol}.")
        return "HOLD", 0, 0, "Error"

def sma_crossover(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    SMA crossover strategy (trend-following):
    - Computes 20-minute (short) and 60-minute (long) SMAs
    - Buys when short SMA > long SMA (bullish crossover)
    - Sells when short SMA < long SMA (bearish crossover)
    - Allocates 10% of available cash per trade
    """
    try:
        # FREQUENCY LOGIC
        if stop_event.wait(60):
            return None, None, None, None

        # HISTORICAL DATA
        df = market_data_client.query_df("""
            SELECT avg(price) AS bar_price
            FROM ticks_db
            WHERE timestamp > now() - INTERVAL 2 HOUR
            GROUP BY toStartOfMinute(timestamp)
            ORDER BY toStartOfMinute(timestamp) DESC
            LIMIT 120
        """)
        prices = df['bar_price'].values
        short_sma = prices[-20:].mean()  # last 20 minutes
        long_sma = prices[-60:].mean()   # last 60 minutes

        # RECENT DATA
        current_price = get_latest_price(consumer)

        # SIGNAL LOGIC
        if short_sma > long_sma:
            decision = "BUY"
        elif short_sma < long_sma:
            decision = "SELL"
        else:
            decision = "HOLD"

        # SIZING LOGIC
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price

        # EXECUTION LOGIC
        execution_logic = (
            f"{decision} signal generated.\n"
            f"Short SMA (20): {short_sma:.2f}, Long SMA (60): {long_sma:.2f}\n"
            f"Current price: {current_price:.2f}."
        )

        return decision, current_price, qty, execution_logic
    except Exception:
        logger.exception(f"Error in SMA crossover logic for {strategy_name}, {symbol}.")
        return "HOLD", 0, 0, "Error"
    
def rsi_reversal(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    """
    RSI-based momentum reversal strategy:
    - Computes 60-minute RSI
    - Buys when RSI < 30 (oversold)
    - Sells when RSI > 70 (overbought)
    - Allocates 10% of available cash per trade
    """
    try:
        if stop_event.wait(60):
            return None, None, None, None

        # HISTORICAL DATA
        df = market_data_client.query_df("""
            SELECT avg(price) AS bar_price
            FROM ticks_db
            WHERE timestamp > now() - INTERVAL 1 HOUR
            GROUP BY toStartOfMinute(timestamp)
            ORDER BY toStartOfMinute(timestamp) DESC
            LIMIT 60
        """)
        prices = df['bar_price'].values
        deltas = prices[1:] - prices[:-1]
        gains = deltas[deltas > 0].sum()
        losses = -deltas[deltas < 0].sum()
        avg_gain = gains / len(prices)
        avg_loss = losses / len(prices)
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        rsi = 100 - (100 / (1 + rs))

        # RECENT DATA
        current_price = get_latest_price(consumer)

        # SIGNAL LOGIC
        if rsi < 30:
            decision = "BUY"
        elif rsi > 70:
            decision = "SELL"
        else:
            decision = "HOLD"

        # SIZING LOGIC
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price

        # EXECUTION LOGIC
        execution_logic = (
            f"{decision} signal generated.\n"
            f"RSI: {rsi:.2f}, Current price: {current_price:.2f}\n"
        )

        return decision, current_price, qty, execution_logic
    except Exception:
        logger.exception(f"Error in RSI reversal logic for {strategy_name}, {symbol}.")
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
            strategy_name="Mean_Reversion",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=mean_reversion,
            strategy_description=(
                f"Buys when the price is >2 standard deviations below its 30-minute mean "
                f"and sells when >2 standard deviations above. Allocates 10% of cash per trade. "
                f"Runs every minute."
            )
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="SMA_Crossover",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=sma_crossover,
            strategy_description=(
                f"Trend-following strategy using 20-minute and 60-minute simple moving averages. "
                f"Buys when short SMA > long SMA, sells when short SMA < long SMA. "
                f"Allocates 10% of cash per trade. Runs every minute."
            )
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="RSI_Reversal",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=rsi_reversal,
            strategy_description=(
                f"Momentum-reversal strategy using RSI. Buys when RSI < 30 (oversold), "
                f"sells when RSI > 70 (overbought). Allocates 10% of cash per trade. "
                f"Runs every minute."
            )
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Breakout",
            starting_cash=100000,
            starting_mv=100000,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=breakout_strategy,
            strategy_description=(
                f"Buys when price breaks above recent 1-hour high, sells when it breaks below "
                f"recent 1-hour low. Allocates 10% of cash per trade. Runs every minute."
            )
        )
    ]