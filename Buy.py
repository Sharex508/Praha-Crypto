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

def get_data_from_binance(filter='USDT'):
    """Fetch price data from Binance API."""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    resp = [d for d in data if filter in d['symbol'] and 'price' in d]
    return resp

def get_coin_limits():
    """Fetch coin limits from the Coinnumber table."""
    connection, cursor = get_db_connection()
    try:
        # Get Coinnumber limits
        sql_limits = "SELECT margin3count, margin5count, margin10count, margin20count, amount FROM Coinnumber LIMIT 1"
        cursor.execute(sql_limits)
        limits = cursor.fetchone()

        coin_limits = {
            "margin3count": int(float(limits[0] or 0)),
            "margin5count": int(float(limits[1] or 0)),
            "margin10count": int(float(limits[2] or 0)),
            "margin20count": int(float(limits[3] or 0)),
            "amount": float(limits[4] or 0.0)
        }

        return coin_limits
    except Exception as e:
        logging.error(f"Error fetching coin limits: {e}")
        return None
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

            api_last_price = float(api_match_data['price'] or 0.0)

            # Convert the margin values to float before comparison
            margin3 = float(db_match_data["margin3"] or 0.0)
            margin5 = float(db_match_data["margin5"] or 0.0)
            margin10 = float(db_match_data["margin10"] or 0.0)
            margin20 = float(db_match_data["margin20"] or 0.0)

            # Use the lock to ensure that updates to coin_limits are thread-safe
            with purchase_lock:
                # Check and purchase at margin3 if limit not reached
                if coin_limits["margin3count"] > 0:
                    if api_last_price >= margin3 and not db_match_data['mar3']:
                        logging.info(f"Purchasing {ele} at 3% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar3')
                        coin_limits["margin3count"] -= 1
                        decrement_coin_limit('margin3count')
                    continue

                # Check and purchase at margin5 if limit not reached
                if coin_limits["margin5count"] > 0:
                    if api_last_price >= margin5 and not db_match_data['mar5']:
                        logging.info(f"Purchasing {ele} at 5% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar5')
                        coin_limits["margin5count"] -= 1
                        decrement_coin_limit('margin5count')
                    continue

                # Check and purchase at margin10 if limit not reached
                if coin_limits["margin10count"] > 0:
                    if api_last_price >= margin10 and not db_match_data['mar10']:
                        logging.info(f"Purchasing {ele} at 10% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar10')
                        coin_limits["margin10count"] -= 1
                        decrement_coin_limit('margin10count')
                    continue

                # Check and purchase at margin20 if limit not reached
                if coin_limits["margin20count"] > 0:
                    if api_last_price >= margin20 and not db_match_data['mar20']:
                        logging.info(f"Purchasing {ele} at 20% margin.")
                        update_margin_status(db_match_data['symbol'], 'mar20')
                        coin_limits["margin20count"] -= 1
                        decrement_coin_limit('margin20count')

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

def decrement_coin_limit(margin_count_field):
    """Decrement the corresponding margin count in the Coinnumber table."""
    connection, cursor = get_db_connection()
    try:
        sql_decrement_margin = f"UPDATE Coinnumber SET {margin_count_field} = {margin_count_field} - 1 WHERE sfid = %s"
        cursor.execute(sql_decrement_margin, ('sfid1',))  # Replace 'sfid1' with the actual sfid if needed
        connection.commit()
        logging.info(f"{margin_count_field} decremented by 1.")
    except Exception as e:
        logging.error(f"Error decrementing {margin_count_field} in Coinnumber table: {e}")
    finally:
        cursor.close()
        connection.close()

def show():
    """Main loop to fetch data, process coins, and handle iterations."""
    while True:
        try:
            logging.info(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Get API data
            api_resp = get_data_from_binance()
            if not api_resp:
                logging.error("Failed to retrieve data from Binance API.")
                time.sleep(60)
                continue

            # Get coin limits
            coin_limits = get_coin_limits()
            if not coin_limits:
                logging.error("Error: Failed to retrieve coin limits from the database.")
                time.sleep(60)
                continue

            # Check if all margins have reached their limits
            if all(count <= 0 for count in [
                coin_limits["margin3count"],
                coin_limits["margin5count"],
                coin_limits["margin10count"],
                coin_limits["margin20count"]
            ]):
                logging.info("All margin counts have reached their limits. Stopping purchases.")
                break  # Exit the loop or sleep until the next day

            # Get DB data
            db_resp = get_results()
            if not db_resp:
                time.sleep(60)
                continue

            # Prepare symbols for processing
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
