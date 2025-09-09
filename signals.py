# signal.py
# Signal generation module, used to generate trading signals based on market data and strategies

import logging, time
from execution import execute_trade
from data import get_latest_price
logger = logging.getLogger(__name__)

# begins generating signals for the execution engine
def generate_signals(stop_event, market_data_client, consumer, trading_data_client):

    logger.info("Signal generator started.")

    try:
        while not stop_event.is_set():
            try:

                # ----------STRATEGY-------------------------------------------
                logger.info("Starting signal generation.")
                # avg price
                recent_avg_df = market_data_client.query_df("SELECT AVG(price) AS recent_avg FROM (SELECT price FROM ticks_db ORDER BY timestamp DESC LIMIT 100) sub")
                recent_avg_price = recent_avg_df.values[0][0]
                logger.info(f"Recent average price: {recent_avg_price}")

                # most recent price
                model_price = get_latest_price(consumer)
                logger.info(f"Latest price: {model_price}")

                qty = 1
                strategy_name = "MeanReversion"
                symbol = "ETH"
                # buy if price above avg, sell if below
                if model_price > recent_avg_price:
                    decision= "SELL"
                if model_price < recent_avg_price:
                    decision= "BUY"

                logger.info("Signal has been generated.")
                
                # ----------STRATEGY-------------------------------------------------
                try:
                    logger.info("Sending signal to execution engine.")
                    execute_trade(trading_data_client, consumer, decision, model_price, qty, strategy_name, symbol)
                    logger.info("Sent signal to execution engine.")

                except Exception:
                    logger.exception("Error sending signal to execution engine.")

                logger.info("Waiting for next signal generation.")
                time.sleep(10)

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
