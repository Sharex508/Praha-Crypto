import time
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
import threading

# Set up logging to show only purchase-related actions
logging.basicConfig(level=logging.INFO)

# Initialize a lock for synchronizing access to the purchase counts
purchase_lock = threading.Lock()

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="HarshaCry")
    return connection, connection.cursor()

def get_data_from_wazirx(filter='USDT'):
    """Fetch price data from WazirX API."""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    return [d for d in data if filter in d['symbol'] and 'price' in d]

def get_coin_limits_and_trading_sums():
    """Fetch coin limits and trading sums from the database."""
    connection, cursor = get_db_connection()
    try:
        # Get Coinnumber limits
        cursor.execute("SELECT margin3count, margin5count, margin10count, margin20count, amount FROM Coinnumber")
        limits = cursor.fetchone()

        coin_limits = {
            "margin3count": int(limits[0] or 0),
            "margin5count": int(limits[1] or 0),
            "margin10count": int(limits[2] or 0),
            "margin20count": int(limits[3] or 0),
            "amount": float(limits[4] or 0.0)
        }

        # Get sum of mar3, mar5, mar10, mar20 from trading table where status != '1' (non-purchased coins)
        cursor.execute("""
            SELECT SUM(CASE WHEN mar3 THEN 1 ELSE 0 END) AS sum_mar3,
                   SUM(CASE WHEN mar5 THEN 1 ELSE 0 END) AS sum_mar5,
                   SUM(CASE WHEN mar10 THEN 1 ELSE 0 END) AS sum_mar10,
                   SUM(CASE WHEN mar20 THEN 1 ELSE 0 END) AS sum_mar20
            FROM trading
            WHERE status != '1'
        """)
        trading_sums = cursor.fetchone()

        trading_summary = {
            "sum_mar3": int(trading_sums[0] or 0),
            "sum_mar5": int(trading_sums[1] or 0),
            "sum_mar10": int(trading_sums[2] or 0),
            "sum_mar20": int(trading_sums[3] or 0)
        }

        return coin_limits, trading_summary
    except Exception as e:
        logging.error(f"Error fetching coin limits and trading sums: {e}")
        return None, None
    finally:
        cursor.close()
        connection.close()

def get_results():
    """Fetch non-purchased trading records from the database."""
    connection, cursor = get_db_connection()
    try:
        cursor.execute("""
            SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
                   mar3, mar5, mar10, mar20
            FROM trading
            WHERE status != '1'
        """)
        results = cursor.fetchall()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'mar3', 'mar5', 'mar10', 'mar20')
        return [dict(zip(keys, obj)) for obj in results]
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def get_diff_of_db_api_values(api_resp):
    """Get the differences between DB and API values and pre-calculate necessary limits and sums."""
    db_resp = get_results()
    coin_limits, trading_summary = get_coin_limits_and_trading_sums()
    
    if not db_resp or not coin_limits or not trading_summary:
        logging.error("Error: Failed to retrieve necessary data from the database.")
        return None, None, None

    return db_resp, coin_limits, trading_summary

def update_margin_status(symbol, margin_level):
    """Update the margin status in the database for a purchased coin."""
    connection, cursor = get_db_connection()
    try:
        cursor.execute(f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s", (symbol,))
        connection.commit()
        logging.info(f"{symbol} purchased at {margin_level}. Status updated to 1.")
    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def task(db_resp, api_resp, coin_limits, trading_summary, data):
    """Process each chunk of data and make purchases based on the margin logic."""
    try:
        for ele in data:
            db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
            if not db_match_data:
                logging.debug(f"DEBUG - Symbol {ele} not found in DB data.")
                continue
            api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
            if not api_match_data:
                logging.debug(f"DEBUG - Symbol {ele} not found in API data.")
                continue

            api_last_price = float(api_match_data['price'] or 0.0)

            margin_levels = {
                "margin3": float(db_match_data.get("margin3", 0.0)),
                "margin5": float(db_match_data.get("margin5", 0.0)),
                "margin10": float(db_match_data.get("margin10", 0.0)),
                "margin20": float(db_match_data.get("margin20", 0.0))
            }

            # Use lock to ensure consistent access to the coin limits
            with purchase_lock:
                for margin_key, margin_value in margin_levels.items():
                    if coin_limits[margin_key + "count"] > 0 and api_last_price >= margin_value:
                        logging.info(f"Purchasing {ele} at {margin_key} margin.")
                        update_margin_status(db_match_data['symbol'], margin_key)
                        coin_limits[margin_key + "count"] -= 1  # Update local limit count
                        break

    except Exception as e:
        logging.error(f"Error in task processing {ele}: {e}")

def show():
    """Main loop to fetch data, process coins, and handle iterations."""
    while True:
        try:
            logging.info(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Get API data
            api_resp = get_data_from_wazirx()
            if not api_resp:
                logging.error("Failed to retrieve data from WazirX API.")
                time.sleep(60)
                continue

            # Get DB and pre-calculated values
            db_resp, coin_limits, trading_summary = get_diff_of_db_api_values(api_resp)
            if not db_resp:
                time.sleep(60)
                continue

            symbols = [obj['symbol'] for obj in db_resp]
            chunk_size = min(20, len(symbols))
            chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

            with ThreadPoolExecutor(max_workers=4) as executor:
                for chunk in chunks:
                    executor.submit(task, db_resp, api_resp, coin_limits, trading_summary, chunk)

            time.sleep(60)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(60)

if __name__ == "__main__":
    show()
