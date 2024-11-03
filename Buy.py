# Buy.py
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
    """Fetch price data from Binance API."""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    resp = [d for d in data if filter in d['symbol'] and 'price' in d]
    return resp

def get_coin_limits_and_trading_sums():
    """Fetch coin limits and trading sums from the database."""
    connection, cursor = get_db_connection()
    try:
        # Get Coinnumber limits
        sql_limits = "SELECT margin3count, margin5count, margin10count, margin20count, amount FROM Coinnumber"
        cursor.execute(sql_limits)
        limits = cursor.fetchone()

        coin_limits = {
            "margin3count": int(float(limits[0] or 0)),
            "margin5count": int(float(limits[1] or 0)),
            "margin10count": int(float(limits[2] or 0)),
            "margin20count": int(float(limits[3] or 0)),
            "amount": float(limits[4] or 0.0)
        }

        # Get sum of mar3, mar5, mar10, mar20 from trading table where status != '1' (non-purchased coins)
        sql_sum = """
        SELECT SUM(CASE WHEN mar3 THEN 1 ELSE 0 END) AS sum_mar3,
               SUM(CASE WHEN mar5 THEN 1 ELSE 0 END) AS sum_mar5,
               SUM(CASE WHEN mar10 THEN 1 ELSE 0 END) AS sum_mar10,
               SUM(CASE WHEN mar20 THEN 1 ELSE 0 END) AS sum_mar20
        FROM trading
        WHERE status != '1'
        """
        cursor.execute(sql_sum)
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
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
               mar3, mar5, mar10, mar20
        FROM trading
        WHERE status != '1'
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20',
                'purchasePrice', 'mar3', 'mar5', 'mar10', 'mar20')

        data = [dict(zip(keys, obj)) for obj in results]

        return data
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def task(db_resp, api_resp, coin_limits, trading_summary, data):
    """Process each chunk of data and make purchases based on the margin logic."""
    try:
        for ele in data:
            db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
            if not db_match_data:
                continue
            api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
            if not api_match_data:
                continue

            api_last_price = float(api_match_data['price'] or 0.0)
            db_price = float(db_match_data.get("intialPrice") or 0.0)

            # Convert the margin values from string to float before comparison
            margin3 = float(db_match_data["margin3"] or 0.0)
            margin5 = float(db_match_data["margin5"] or 0.0)
            margin10 = float(db_match_data["margin10"] or 0.0)
            margin20 = float(db_match_data["margin20"] or 0.0)

            # Use the lock to ensure that updates to the trading_summary are thread-safe
            with purchase_lock:
                # Check and purchase at margin3 if limit not reached
                if trading_summary["sum_mar3"] < coin_limits["margin3count"]:
                    if api_last_price >= margin3 and not db_match_data['mar3']:
                        logging.info(f"Purchasing {ele} at 3% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar3')
                        trading_summary["sum_mar3"] += 1
                    continue

                # Check and purchase at margin5 if limit not reached
                if trading_summary["sum_mar5"] < coin_limits["margin5count"]:
                    if api_last_price >= margin5 and not db_match_data['mar5']:
                        logging.info(f"Purchasing {ele} at 5% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar5')
                        trading_summary["sum_mar5"] += 1
                    continue

                # Check and purchase at margin10 if limit not reached
                if trading_summary["sum_mar10"] < coin_limits["margin10count"]:
                    if api_last_price >= margin10 and not db_match_data['mar10']:
                        logging.info(f"Purchasing {ele} at 10% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar10')
                        trading_summary["sum_mar10"] += 1
                    continue

                # Check and purchase at margin20 if limit not reached
                if trading_summary["sum_mar20"] < coin_limits["margin20count"]:
                    if api_last_price >= margin20 and not db_match_data['mar20']:
                        logging.info(f"Purchasing {ele} at 20% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar20')
                        trading_summary["sum_mar20"] += 1

    except Exception as e:
        logging.error(f"Error in task processing {ele}: {e}")

def update_margin_status(symbol, margin_level):
    """Update the margin status in the database for a purchased coin."""
    connection, cursor = get_db_connection()
    try:
        sql_update_trading = f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s"
        cursor.execute(sql_update_trading, (symbol,))
        connection.commit()
        logging.info(f"{symbol} purchased at {margin_level}. Status updated to 1.")
    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
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

def show():
    """Main loop to fetch data, process coins, and handle iterations."""
    while True:
        try:
            logging.info(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Get API data
            api_resp = get_data_from_wazirx()
            if not api_resp:
                logging.error("Failed to retrieve data from Binance API.")
                time.sleep(60)
                continue

            # Get DB and pre-calculated values
            db_resp, coin_limits, trading_summary = get_diff_of_db_api_values(api_resp)
            if not db_resp:
                time.sleep(60)
                continue

            # Prepare symbols for processing
            dicts_data = [obj['symbol'] for obj in db_resp]
            chunk_size = min(20, len(dicts_data))
            chunks = [dicts_data[i:i + chunk_size] for i in range(0, len(dicts_data), chunk_size)]

            with ThreadPoolExecutor(max_workers=4) as executor:
                for chunk in chunks:
                    executor.submit(task, db_resp, api_resp, coin_limits, trading_summary, chunk)

            time.sleep(60)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(60)

if __name__ == "__main__":
    show()
