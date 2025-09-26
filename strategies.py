# strategies.py
# contains all strategy functions, and packages them so they can be sent to main.py

import logging
from data import get_latest_price
from strategy_template import StrategyTemplate
from portfolio import get_cash_balance
from ml_functions import get_production_data_seconds, build_features, get_ml_model
from config import MONITOR_FREQUENCY
logger = logging.getLogger(__name__)

def ml_random_forest_second_ticks(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    try:
        # SET TRADE FREQUENCY
        if stop_event.wait(15):
            return None, None, None, None

        # GET PRODUCTION DATA
        prod_df = get_production_data_seconds(market_data_client)
        feature_df = build_features(prod_df)
        feature_df_clean = feature_df.drop(columns=["price", "ts"]).iloc[[-1]]

        # GET MODEL
        s3_key = "models/random_forest_-_second_features.pkl"
        local_path = "models/random_forest_-_second_features.pkl"
        ml_model = get_ml_model(s3_key, local_path)

        # PREDICT FROM MODEL
        prediction = ml_model.predict(feature_df_clean)[0]
        if prediction == 2:
            decision = "BUY"
        elif prediction == 0:
            decision = "SELL"
        else:
            decision = "HOLD"

        # DETERMINE TRADE SIZE
        current_price = get_latest_price(consumer)
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price if decision in ["BUY", "SELL"] else 0

        # RECORD EXECUTION LOGIC
        execution_logic = (
            f"{strategy_name} decision: {decision}\n"
            f"Features: {feature_df_clean.to_dict(orient='records')[0]}\n"
            f"Current price: {current_price:.2f}"
        )

        #SEND TRADE TO EXECTUION ENGINE
        return decision, current_price, qty, execution_logic

    except Exception as e:
        logger.exception(f"Error in {strategy_name} strategy: {e}")
        return "HOLD", 0, 0, "Error"
    
def gradient_boosting_second_ticks(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    try:
        # SET TRADE FREQUENCY
        if stop_event.wait(15):
            return None, None, None, None

        # GET PRODUCTION DATA
        prod_df = get_production_data_seconds(market_data_client)
        feature_df = build_features(prod_df)
        feature_df_clean = feature_df.drop(columns=["price", "ts"]).iloc[[-1]]

        # GET MODEL
        s3_key = "models/gradient_boosting_-_second_features.pkl"
        local_path = "models/gradient_boosting_-_second_features.pkl"
        ml_model = get_ml_model(s3_key, local_path)

        # PREDICT FROM MODEL
        prediction = ml_model.predict(feature_df_clean)[0]
        if prediction == 2:
            decision = "BUY"
        elif prediction == 0:
            decision = "SELL"
        else:
            decision = "HOLD"

        # DETERMINE TRADE SIZE
        current_price = get_latest_price(consumer)
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price if decision in ["BUY", "SELL"] else 0

        # RECORD EXECUTION LOGIC
        execution_logic = (
            f"{strategy_name} decision: {decision}\n"
            f"Features: {feature_df_clean.to_dict(orient='records')[0]}\n"
            f"Current price: {current_price:.2f}"
        )

        #SEND TRADE TO EXECTUION ENGINE
        return decision, current_price, qty, execution_logic

    except Exception as e:
        logger.exception(f"Error in {strategy_name} strategy: {e}")
        return "HOLD", 0, 0, "Error"
    

def logistic_regression_second_ticks(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    try:
        # SET TRADE FREQUENCY
        if stop_event.wait(15):
            return None, None, None, None

        # GET PRODUCTION DATA
        prod_df = get_production_data_seconds(market_data_client)
        feature_df = build_features(prod_df)
        feature_df_clean = feature_df.drop(columns=["price", "ts"]).iloc[[-1]]

        # GET MODEL
        s3_key = "models/logistic_regression_-_second_features.pkl"
        local_path = "models/logistic_regression_-_second_features.pkl"
        ml_model = get_ml_model(s3_key, local_path)

        # PREDICT FROM MODEL
        prediction = ml_model.predict(feature_df_clean)[0]
        if prediction == 2:
            decision = "BUY"
        elif prediction == 0:
            decision = "SELL"
        else:
            decision = "HOLD"

        # DETERMINE TRADE SIZE
        current_price = get_latest_price(consumer)
        cash_balance = get_cash_balance(trading_data_client, strategy_name, symbol)
        allocation_pct = 0.10
        qty = (cash_balance * allocation_pct) / current_price if decision in ["BUY", "SELL"] else 0

        # RECORD EXECUTION LOGIC
        execution_logic = (
            f"{strategy_name} decision: {decision}\n"
            f"Features: {feature_df_clean.to_dict(orient='records')[0]}\n"
            f"Current price: {current_price:.2f}"
        )

        #SEND TRADE TO EXECTUION ENGINE
        return decision, current_price, qty, execution_logic

    except Exception as e:
        logger.exception(f"Error in {strategy_name} strategy: {e}")
        return "HOLD", 0, 0, "Error"

def long_only(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event):
    try:
        # SET TRADE FREQUENCY
        if stop_event.wait(60):
            return None, None, None, None

        # GET PRODUCTION DATA

        # GET MODEL
        decision = "HOLD"

        # PREDICT FROM MODEL

        # DETERMINE TRADE SIZE
        current_price = get_latest_price(consumer)
        qty = 0

        # RECORD EXECUTION LOGIC
        execution_logic = (
            f"{strategy_name} decision: {decision}\n"
        )
        
        #SEND TRADE TO EXECTUION ENGINE
        return decision, current_price, qty, execution_logic

    except Exception:
        logger.exception(f"Error in {strategy_name} strategy.")
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
            strategy_name="Random_Forest",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=ml_random_forest_second_ticks,
            strategy_description=(
                "Random Forest - Second Ticks"
            ),
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Gradient_Boosting",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=gradient_boosting_second_ticks,
            strategy_description=(
                "Gradient Boosting - Second Ticks"
            ),
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Logistic_Regression",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=MONITOR_FREQUENCY,
            strategy_function=logistic_regression_second_ticks,
            strategy_description=(
                "Logistic Regression - Second Ticks"
            )        
        )
    ]