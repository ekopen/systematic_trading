# risk.py
# risk engine module, used to manage risk parameters and ensure trades are executed within defined risk limits

import logging
from config import MAX_DRAWDOWN, MAX_ALLOCATION
from portfolio import get_cash_balance, get_qty_balance
logger = logging.getLogger(__name__)

def check_position_size(client, symbol, qty, price, strategy_name, max_allocation=MAX_ALLOCATION):
    logger.info("Checking position size for trade.")
    try:
        rows = client.query(f"""
            SELECT portfolio_value 
            FROM portfolio_db_key 
            WHERE strategy_name = '{strategy_name}' AND symbol = '{symbol}'
        """).result_rows
        portfolio_value = rows[0][0]
        trade_value = qty * price
        if trade_value > max_allocation * portfolio_value:
            msg = f"Trade value {trade_value:.2f} exceeds max allocation {max_allocation*100:.1f}% of portfolio {portfolio_value:.2f}"
            logger.warning(msg)
            return False, msg
    except Exception:
        logger.exception("Error checking position size.")
        return False, "Error in position size check"
    return True, "Position size ok"


def check_cash_balance(client, qty, direction, price, strategy_name, symbol):
    logger.info("Checking cash balance for trade.")
    try:
        cash_balance = get_cash_balance(client, strategy_name, symbol)
        trade_cost = qty * price * direction
        if trade_cost > cash_balance:
            msg = f"Not enough cash. Needed {trade_cost:.2f}, available {cash_balance:.2f}"
            logger.warning(msg)
            return False, msg
    except Exception:
        logger.exception("Error checking cash balance.")
        return False, "Error in cash balance check"
    return True, "Cash balance ok"

def check_qty_balance(client, qty, direction, strategy_name, symbol):
    logger.info("Checking quantity balance for trade.")
    try:
        quantity = get_qty_balance(client, strategy_name, symbol)
        traded_qty = qty * direction
        if -traded_qty > quantity:
            msg = f"Not enough quantity. Needed {-traded_qty:.2f}, available {quantity:.2f}"
            logger.warning(msg)
            return False, msg
    except Exception:
        logger.exception("Error checking quantity balance.")
        return False, "Error in quantity balance check"
    return True, "Quantity balance ok"


def check_max_drawdown(client, strategy_name, symbol, max_drawdown=MAX_DRAWDOWN):
    logger.info("Checking max drawdown for trade.")
    try:
        rows = client.query(f"""
            WITH
                (SELECT MIN(portfolio_value) 
                 FROM portfolio_db_ts 
                 WHERE strategy_name = '{strategy_name}' AND symbol = '{symbol}') AS min_val,
                (SELECT MAX(portfolio_value) 
                 FROM portfolio_db_ts 
                 WHERE strategy_name = '{strategy_name}' AND symbol = '{symbol}') AS max_val,
                (SELECT portfolio_value 
                 FROM portfolio_db_ts 
                 WHERE strategy_name = '{strategy_name}' AND symbol = '{symbol}' 
                 ORDER BY time DESC LIMIT 1) AS latest_val
            SELECT min_val, max_val, latest_val
        """).result_rows

        if rows:
            min_val, max_val, latest_val = rows[0]
            if max_val and max_val > 0:
                drawdown = (max_val - latest_val) / max_val
                if drawdown > max_drawdown:
                    msg = f"Max drawdown {drawdown:.2%} exceeded limit {max_drawdown:.0%}"
                    logger.warning(msg)
                    return False, msg
    except Exception:
        logger.exception("Error checking max drawdown.")
        return False, "Error in drawdown check"
    return True, "Drawdown ok"


def run_risk_checks(client, symbol, qty, direction, model_price, strategy_name):
    checks = [
        check_position_size(client, symbol, qty, model_price, strategy_name),
        check_cash_balance(client, qty, direction,
         model_price, strategy_name, symbol),
        check_qty_balance(client, qty, direction,
         strategy_name, symbol),
        check_max_drawdown(client, strategy_name, symbol)
    ]

    failures = []
    for success, msg in checks:
        if not success:
            failures.append(msg)
    all_passed = len(failures) == 0
    return all_passed, failures
