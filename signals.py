# signal.py
# Signal generation module, used to generate trading signals based on market data and strategies

import logging
from execution import execute_trade
logger = logging.getLogger(__name__)

# begins generating signals for the execution engine
def generate_signals(stop_event, market_data_client, consumer, trading_data_client, strategy_name, symbol, strategy_function):

    logger.info("Signal generator started.")

    try:
        while not stop_event.is_set():
            try:
                logger.info(f"Waiting for next signal generation for {symbol}, {strategy_name}.")
                decision, model_price, qty, execution_logic = strategy_function(market_data_client, consumer, trading_data_client, strategy_name, symbol, stop_event)
                logger.info(f"Signal generated for {symbol}, {strategy_name}.")
                try:
                    logger.info("Sending signal to execution engine.")
                    execute_trade(trading_data_client, consumer, decision, model_price, qty, strategy_name, symbol, execution_logic)
                    logger.info("Sent signal to execution engine.")

                except Exception:
                    logger.exception("Error sending signal to execution engine.")
            except Exception:
                logger.exception("Error in signal generator loop")

    except Exception:
        logger.exception("Error in signal generator")
    finally:
        try:
            logger.info("Signal generator shutting down.")
            consumer.close()
        except Exception:
            logger.exception("Error during signal generator shutdown.")
        logger.info("Signal generator stopped.")
