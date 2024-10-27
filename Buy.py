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
    """Establish and return a connection to the PostgreSQL database."""
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
        logging.error(f"Error connecting to database: {e}")
        return None

def fetch_all_data_from_table(table_name):
    """Fetch and display all data from a specific table."""
    connection = get_db_connection()
    if connection is None:
        logging.error("Failed to connect to database.")
        return []

    try:
        with connection:
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

    for coin in db_resp:
        # Stop if we reach the purchase limit
        if purchased_count >= PURCHASE_LIMIT:
            logging.info(f"Purchase limit of {PURCHASE_LIMIT} reached. Stopping purchases.")
            return

        symbol = coin["symbol"]
        api_data = next((item for item in api_resp if item["symbol"] == symbol), None)

        if not api_data:
            logging.debug(f"DEBUG - Symbol {symbol} not found in API data.")
            continue

        api_last_price = float(api_data['price'])
        db_price = float(coin.get("intialprice", 0))

        logging.debug(f"Processing {symbol} - Last Price: {api_last_price}, DB Price: {db_price}")

        # Margin logic for mar3, mar5, mar10, mar20
        margin3 = float(coin.get("margin3", 0))
        margin3count = int(coin.get("margin3count", 0) or 0)  # Ensure None becomes 0
        margin5 = float(coin.get("margin5", 0))
        margin5count = int(coin.get("margin5count", 0) or 0)
        margin10 = float(coin.get("margin10", 0))
        margin10count = int(coin.get("margin10count", 0) or 0)
        margin20 = float(coin.get("margin20", 0))
        margin20count = int(coin.get("margin20count", 0) or 0)

        logging.debug(f"DEBUG - Margins for {symbol}: mar3={margin3}, mar5={margin5}, mar10={margin10}, mar20={margin20}")
        logging.debug(f"DEBUG - Counts for {symbol}: margin3count={margin3count}, margin5count={margin5count}, margin10count={margin10count}, margin20count={margin20count}")

        # Keep track of the highest margin
        highest_margin_level = None
        highest_margin = 0

        # Check which margin levels qualify, and ensure that the count is greater than 0
        if not coin['mar3'] and api_last_price >= margin3 and margin3count > 0:
            logging.debug(f"DEBUG - {symbol} qualifies for mar3: API Price={api_last_price} >= Margin3={margin3}")
            highest_margin_level = 'mar3'
            highest_margin = margin3
        elif not coin['mar5'] and api_last_price >= margin5 and margin5count > 0:
            logging.debug(f"DEBUG - {symbol} qualifies for mar5: API Price={api_last_price} >= Margin5={margin5}")
            highest_margin_level = 'mar5'
            highest_margin = margin5
        elif not coin['mar10'] and api_last_price >= margin10 and margin10count > 0:
            logging.debug(f"DEBUG - {symbol} qualifies for mar10: API Price={api_last_price} >= Margin10={margin10}")
            highest_margin_level = 'mar10'
            highest_margin = margin10
        elif not coin['mar20'] and api_last_price >= margin20 and margin20count > 0:
            logging.debug(f"DEBUG - {symbol} qualifies for mar20: API Price={api_last_price} >= Margin20={margin20}")
            highest_margin_level = 'mar20'
            highest_margin = margin20

        # Purchase the coin at the highest qualifying margin
        if highest_margin_level:
            logging.debug(f"Purchasing {symbol} at {highest_margin_level} margin. Last price: {api_last_price}, Required: {highest_margin}")
            purchased_count += 1
            update_margin_status(coin['symbol'], highest_margin_level)
            reset_margin_count(coin['symbol'], highest_margin_level)  # Set margin count to 0 after purchase

        logging.debug(f"DEBUG - {symbol} processed, total purchased: {purchased_count}")

def reset_margin_count(symbol, margin_level):
    """Reset the margin count of a coin after purchasing to 0."""
    connection = get_db_connection()
    if connection is None:
        logging.error("Error: Failed to connect to the database.")
        return
    try:
        # Set the count of the respective margin (e.g., margin3count, margin5count) to 0 after purchase
        margin_count_field = f"{margin_level}count"
        sql_update = f"UPDATE trading SET {margin_count_field} = 0 WHERE symbol = %s"
        with connection.cursor() as cursor:
            cursor.execute(sql_update, (symbol,))
            connection.commit()
        logging.info(f"Reset {margin_count_field} to 0 for {symbol}.")
    except Exception as e:
        logging.error(f"Error resetting margin count for {symbol}: {e}")
    finally:
        connection.close()


def update_margin_status(symbol, margin_level):
    """Update the margin status in the database for a purchased coin."""
    connection = get_db_connection()
    if connection is None:
        logging.error("Error: Failed to connect to the database.")
        return
    try:
        # Update the margin level to TRUE and mark the status as purchased (status = '1')
        sql_update = f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s"
        with connection.cursor() as cursor:
            cursor.execute(sql_update, (symbol,))
            connection.commit()
        logging.info(f"Updated {symbol} with margin level {margin_level}.")
    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
    finally:
        connection.close()


def show():
    """Main function to initiate data fetching and processing."""
    global purchased_count  # Reset the purchase count at the beginning

    while purchased_count < PURCHASE_LIMIT:
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
