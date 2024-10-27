import time
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Global purchase limit
PURCHASE_LIMIT = 20
purchased_count = 0  # Counter for the total number of coins purchased

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database with retry."""
    retries = 3
    for attempt in range(retries):
        try:
            connection = psycopg2.connect(
                user="postgres",
                password="Harsha508",
                host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                port="5432",
                database="HarshaCry"
            )
            logging.info("Database connection established.")
            return connection
        except Exception as e:
            logging.error(f"Error connecting to database: {e}. Retrying {attempt + 1}/{retries}...")
            time.sleep(5)
    logging.error("Failed to connect to the database after retries.")
    return None

def fetch_all_data_from_table(table_name):
    """Fetch and display all data from a specific table."""
    connection = get_db_connection()
    if connection is None:
        logging.error("Failed to connect to database.")
        return []

    try:
        with connection.cursor() as cursor:
            # Fetch all rows from the specified table where status is not 1 (non-purchased)
            cursor.execute(f"SELECT * FROM {table_name} WHERE status != '1'")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            
            logging.debug(f"DEBUG - Fetched {len(data)} records from table '{table_name}'")
            return data
    except Exception as e:
        logging.error(f"Error fetching data from table {table_name}: {e}")
        return []
    finally:
        connection.close()
        logging.info("Database connection closed.")

def get_data_from_wazirx(filter='USDT'):
    """Fetch price data from WazirX API."""
    try:
        data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
        filtered_data = [d for d in data if filter in d['symbol']]
        logging.debug(f"DEBUG - Fetched {len(filtered_data)} symbols from API with filter '{filter}'.")
        return filtered_data
    except Exception as e:
        logging.error(f"Error fetching data from WazirX: {e}")
        return []

def task(db_resp, api_resp):
    """Process the data and make purchases based on the highest qualifying margin."""
    global purchased_count  # To track the number of purchases
    updates = []  # To store batch updates

    for coin in db_resp:
        if purchased_count >= PURCHASE_LIMIT:
            logging.info(f"Purchase limit of {PURCHASE_LIMIT} reached. Stopping purchases.")
            break

        symbol = coin["symbol"]
        api_data = next((item for item in api_resp if item["symbol"] == symbol), None)

        if not api_data:
            logging.debug(f"DEBUG - Symbol {symbol} not found in API data.")
            continue

        api_last_price = float(api_data['price'])
        db_price = float(coin.get("intialprice", 0))

        logging.debug(f"Processing {symbol} - Last Price: {api_last_price}, DB Price: {db_price}")

        # Prioritize higher margins first (mar20, mar10, mar5, mar3)
        if not coin['mar20'] and api_last_price >= float(coin.get("margin20", 0)):
            updates.append(('mar20', coin['symbol']))
        elif not coin['mar10'] and api_last_price >= float(coin.get("margin10", 0)):
            updates.append(('mar10', coin['symbol']))
        elif not coin['mar5'] and api_last_price >= float(coin.get("margin5", 0)):
            updates.append(('mar5', coin['symbol']))
        elif not coin['mar3'] and api_last_price >= float(coin.get("margin3", 0)):
            updates.append(('mar3', coin['symbol']))

        purchased_count += 1
        logging.debug(f"DEBUG - {symbol} processed, total purchased: {purchased_count}")

    # Perform batch updates
    if updates:
        update_margin_status_batch(updates)

def update_margin_status_batch(updates):
    """Batch update margin status for multiple coins."""
    if not updates:
        return

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                sql_update = "UPDATE trading SET {} = TRUE, status = '1' WHERE symbol = %s"
                for margin_level, symbol in updates:
                    cursor.execute(sql_update.format(margin_level), (symbol,))
                connection.commit()
        logging.info(f"Batch updated {len(updates)} records.")
    except Exception as e:
        logging.error(f"Error in batch update: {e}")

def show():
    """Main function to initiate data fetching and processing."""
    global purchased_count  # Reset the purchase count at the beginning

    while purchased_count < PURCHASE_LIMIT:
        purchased_count = 0  # Reset purchased count each time show() starts

        logging.info(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Get API data
        api_resp = get_data_from_wazirx()
        if not api_resp:
            logging.error("Failed to retrieve data from WazirX API.")
            time.sleep(60)
            continue

        # Get DB data
        db_resp = fetch_all_data_from_table("trading")
        if not db_resp:
            logging.error("No data retrieved from the database.")
            time.sleep(60)
            continue

        # Process the data
        task(db_resp, api_resp)

        # Check if the purchase limit has been reached
        if purchased_count >= PURCHASE_LIMIT:
            logging.info(f"Purchase limit of {PURCHASE_LIMIT} coins reached. Exiting loop.")
            break

        # Wait for 60 seconds before the next iteration
        time.sleep(60)

if __name__ == "__main__":
    show()
