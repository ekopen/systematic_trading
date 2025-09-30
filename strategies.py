# strategies.py
# contains all strategy functions, and packages them so they can be sent to main.py

from strategy_template import StrategyTemplate

def get_strategies(stop_event):
    return [
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Long Only",
            starting_cash=0,
            starting_mv=200000,
            monitor_frequency=60,
            strategy_description=("Benchmark strategy."),
            execution_frequency=30,
            s3_key = "models/long_only.pkl",
            local_path = "models/long_only.pkl"
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Random_Forest",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=60,
            strategy_description=("Splits data with many trees."),
            execution_frequency=30,
            s3_key = "models/random_forest.pkl",
            local_path = "models/random_forest.pkl"
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Gradient_Boosting",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=60,
            strategy_description=("Builds strong ensemble step by step."),
            execution_frequency=30,
            s3_key = "models/gradient_boosting.pkl",
            local_path = "models/gradient_boosting.pkl"
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="Logistic_Regression",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=60,
            strategy_description=("Simple linear baseline."),
            execution_frequency=15,
            s3_key = "models/logistic_regression.pkl",
            local_path = "models/logistic_regression.pkl"
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="LSTM",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=60,
            strategy_description=("Learns patterns over time in sequences."),
            execution_frequency=30,
            s3_key = "models/lstm.pkl",
            local_path = "models/lstm.pkl"
        ),
        StrategyTemplate(
            stop_event=stop_event,
            kafka_topic="price_ticks",
            symbol="ETH",
            strategy_name="SVM",
            starting_cash=200000,
            starting_mv=0,
            monitor_frequency=60,
            strategy_description=("Finds complex decision boundaries."),
            execution_frequency=30,
            s3_key = "models/svm_(rbf_kernel).pkl",
            local_path = "models/svm_(rbf_kernel).pkl"
        ),
        
    ]