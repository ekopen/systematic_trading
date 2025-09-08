# signal_engine.py
# Signal generation module

import logging, time, json
from execution import execute_trade
from data import get_latest_price
logger = logging.getLogger(__name__)


# begins generating signals for the execution engine
def generate_signals(stop_event, market_data_client, consumer, trading_data_client):


    logger.info("Signal generator started.")

    try:
        while not stop_event.is_set():
            try:

                # Strategy implemented below:
                logger.info("Starting signal generation.")
                # avg price
                recent_avg_df = market_data_client.query_df("SELECT AVG(price) AS recent_avg FROM (SELECT price FROM ticks_db ORDER BY timestamp DESC LIMIT 100) sub")
                recent_avg_price = recent_avg_df.values[0][0]
                logger.info(f"Recent average price: {recent_avg_price}")

                # most recent price
                msg = get_latest_price(consumer)
                current_price = json.loads(msg.value.decode("utf-8")).get("price")
                logger.info(f"Latest price: {current_price}")
            
                # realistic execution price (one second delay) #MAYBE MOVE THIS TO EXECUTION!
                time.sleep(1)
                msg = get_latest_price(consumer)
                execution_price = json.loads(msg.value.decode("utf-8")).get("price")

                qty = 1
                strategy_name = "MeanReversion"
                symbol = "ETH"
                # buy if price above avg, sell if below
                if current_price > recent_avg_price:
                    execute_trade(trading_data_client, "SELL", execution_price, qty, strategy_name, symbol)
                if current_price < recent_avg_price:
                    execute_trade(trading_data_client, "BUY", execution_price, qty, strategy_name, symbol)

                logger.info("Signal has been generated and sent to execution engine.")
                # time out parameter
                time.sleep(5)
                logger.info("Waiting for next signal generation.")

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
