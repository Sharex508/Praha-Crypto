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

def get_coin_limits():
    """Fetch and return daily coin limits from the Coinnumber table."""
    connection, cursor = get_db_connection()
    try:
        cursor.execute("SELECT margin3count, margin5count, margin10count, margin20count FROM Coinnumber")
        limits = cursor.fetchone()
        return {
            "margin3count": int(limits[0] or 0),
            "margin5count": int(limits[1] or 0),
            "margin10count": int(limits[2] or 0),
            "margin20count": int(limits[3] or 0)
        }
    except Exception as e:
        logging.error(f"Error fetching coin limits: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def update_coin_limit(margin_level):
    """Decrement the specified margin count in Coinnumber table."""
    connection, cursor = get_db_connection()
    try:
        sql_update = f"UPDATE Coinnumber SET {margin_level} = {margin_level} - 1 WHERE {margin_level} > 0"
        cursor.execute(sql_update)
        connection.commit()
        logging.info(f"Updated {margin_level} in Coinnumber.")
    except Exception as e:
        logging.error(f"Error updating coin limit for {margin_level}: {e}")
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

def task(db_resp, api_resp, coin_limits, data):
    """Process each chunk of data and make purchases based on the margin logic."""
    try:
        for ele in data:
            db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
            if not db_match_data:
                continue
            api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
            if not api_match_data:
                continue

            try:
                api_last_price = float(api_match_data['price'] or 0.0)
            except ValueError:
                logging.error(f"Invalid price for {ele}. Skipping.")
                continue

            # Ensure that margin values are float-compatible
            margin_levels = {}
            for margin in ["margin3", "margin5", "margin10", "margin20"]:
                try:
                    margin_levels[margin] = float(db_match_data.get(margin, 0.0))
                except ValueError:
                    logging.error(f"Invalid {margin} value for {ele}. Skipping.")
                    continue

            # Use lock to ensure consistent access to the coin limits
            with purchase_lock:
                for margin_key, margin_value in margin_levels.items():
                    if coin_limits[margin_key] > 0 and api_last_price >= margin_value:
                        logging.info(f"Purchasing {ele} at {margin_key} margin.")
                        update_margin_status(db_match_data['symbol'], margin_key)
                        update_coin_limit(margin_key)
                        coin_limits[margin_key] -= 1  # Update local limit count
                        break

    except Exception as e:
        logging.error(f"Error in task processing {ele}: {e}")

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

def get_diff_of_db_api_values(api_resp):
    """Get the differences between DB and API values and pre-calculate necessary limits and sums."""
    db_resp = get_results()
    coin_limits = get_coin_limits()
    if not db_resp or not coin_limits:
        logging.error("Error: Failed to retrieve necessary data from the database.")
        return None, None
    return db_resp, coin_limits

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
            db_resp, coin_limits = get_diff_of_db_api_values(api_resp)
            if not db_resp:
                time.sleep(60)
                continue

            dicts_data = [obj['symbol'] for obj in db_resp]
            chunk_size = min(20, len(dicts_data))
            chunks = [dicts_data[i:i + chunk_size] for i in range(0, len(dicts_data), chunk_size)]

            with ThreadPoolExecutor(max_workers=4) as executor:
                for chunk in chunks:
                    executor.submit(task, db_resp, api_resp, coin_limits, chunk)

            time.sleep(60)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(60)

if __name__ == "__main__":
    show()
