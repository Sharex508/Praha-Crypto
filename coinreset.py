# coinreset.py

import time
import logging
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_batch
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging to display both INFO and ERROR messages with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("coinreset.log"),
        logging.StreamHandler()
    ]
)

def get_database_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        connection = psycopg2.connect(
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD''Harsha508'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'HarshaCry'),
        )
        logging.info("Successfully connected to the PostgreSQL database.")
        return connection, connection.cursor()
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")
        raise

def truncate_tables():
    """Truncate the trading and Coinnumber tables."""
    try:
        connection, cursor = get_database_connection()
        with connection:
            with cursor:
                cursor.execute("TRUNCATE TABLE trading CASCADE;")
                cursor.execute("TRUNCATE TABLE Coinnumber CASCADE;")
                logging.info("Tables 'trading' and 'Coinnumber' truncated successfully.")
    except psycopg2.Error as e:
        logging.error(f"Error truncating tables: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def create_tables():
    """Create the trading and Coinnumber tables if they do not exist."""
    try:
        connection, cursor = get_database_connection()
        with connection:
            with cursor:
                # Create trading table with corrected spelling and appropriate data types
                create_trading_table_query = '''
                    CREATE TABLE IF NOT EXISTS trading (
                        symbol                          TEXT    NOT NULL,
                        initialPrice                    FLOAT,
                        highPrice                       FLOAT,
                        lastPrice                       FLOAT,
                        margin3                         FLOAT,
                        margin5                         FLOAT,
                        margin10                        FLOAT,
                        margin20                        FLOAT,
                        purchasePrice                   FLOAT,
                        stopLossPrice                   FLOAT,
                        mar3                            BOOLEAN DEFAULT FALSE,
                        mar5                            BOOLEAN DEFAULT FALSE,
                        mar10                           BOOLEAN DEFAULT FALSE,
                        mar20                           BOOLEAN DEFAULT FALSE,
                        created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status                          TEXT DEFAULT '0',
                        last_notified_percentage        FLOAT DEFAULT 0.0,
                        last_notified_decrease_percentage FLOAT DEFAULT 0.0
                    );
                '''
                cursor.execute(create_trading_table_query)
                logging.info("Table 'trading' created successfully or already exists.")

                # Create Coinnumber table with appropriate data types
                create_coinnumber_table_query = '''
                    CREATE TABLE IF NOT EXISTS Coinnumber (
                        sfid            TEXT PRIMARY KEY,
                        margin3count    INTEGER,
                        margin5count    INTEGER,
                        margin10count   INTEGER,
                        margin20count   INTEGER,
                        amount          FLOAT
                    );
                '''
                cursor.execute(create_coinnumber_table_query)
                logging.info("Table 'Coinnumber' created successfully or already exists.")
    except (Exception, Error) as error:
        logging.error(f"Error creating tables: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def getall_data(filter='USDT'):
    """Fetch trading data from Binance API."""
    try:
        response = requests.get('https://api.binance.com/api/v3/ticker/price', timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info("Successfully fetched data from Binance API.")
    except requests.RequestException as e:
        logging.error(f"Error fetching data from Binance API: {e}")
        return []

    # Process data and calculate margins and stop-loss price
    trading_data = []
    for d in data:
        if filter in d['symbol']:
            try:
                price = float(d['price'])
                trading_entry = {
                    'symbol': d['symbol'],
                    'initialPrice': price,
                    'highPrice': price,
                    'lastPrice': price,
                    'margin3': price * 1.03,
                    'margin5': price * 1.05,
                    'margin10': price * 1.10,
                    'margin20': price * 1.20,
                    'purchasePrice': None,  # To be updated upon purchase
                    'stopLossPrice': None   # To be set upon purchase (10% increase as per requirement)
                }
                trading_data.append(trading_entry)
            except ValueError as ve:
                logging.error(f"Error processing price for symbol {d['symbol']}: {ve}")

    logging.info(f"Processed {len(trading_data)} trading records from Binance API.")
    return trading_data

def insert_data_db(trading_data):
    """Insert trading data into the PostgreSQL database."""
    if not trading_data:
        logging.warning("No trading data to insert.")
        return

    try:
        connection, cursor = get_database_connection()
        with connection:
            with cursor:
                # Define the columns to insert
                trading_columns = [
                    'symbol', 'initialPrice', 'highPrice', 'lastPrice',
                    'margin3', 'margin5', 'margin10', 'margin20',
                    'purchasePrice', 'stopLossPrice'
                ]
                # Prepare the INSERT query
                trading_query = f"""
                    INSERT INTO trading ({', '.join(trading_columns)})
                    VALUES ({', '.join(['%s'] * len(trading_columns))})
                """
                # Prepare the data for insertion
                trading_values = [
                    (
                        obj['symbol'],
                        obj['initialPrice'],
                        obj['highPrice'],
                        obj['lastPrice'],
                        obj['margin3'],
                        obj['margin5'],
                        obj['margin10'],
                        obj['margin20'],
                        obj['purchasePrice'],
                        obj['stopLossPrice']
                    )
                    for obj in trading_data
                ]
                # Execute batch insertion
                execute_batch(cursor, trading_query, trading_values, page_size=100)
                logging.info(f"Inserted {len(trading_values)} records into 'trading' table successfully.")
    except Exception as error:
        logging.error(f"Error inserting data into 'trading' table: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_coinnumber_data():
    """Prepare Coinnumber data to insert into the database."""
    # Define the data you want to insert into Coinnumber
    # Replace this with your actual data source or logic
    coinnumber_data = [
        {
            'sfid': '1',
            'margin3count': 5,
            'margin5count': 5,
            'margin10count': 5,
            'margin20count': 5,
            'amount': 10.0
        },
        {
            'sfid': '2',
            'margin3count': 3,
            'margin5count': 3,
            'margin10count': 3,
            'margin20count': 3,
            'amount': 20.0
        },
        # Add more entries as needed
    ]
    return coinnumber_data

def insert_coinnumber_data(coinnumber_data):
    """Insert data into Coinnumber table."""
    if not coinnumber_data:
        logging.warning("No Coinnumber data to insert.")
        return

    try:
        connection, cursor = get_database_connection()
        with connection:
            with cursor:
                # Define the columns to insert
                coinnumber_columns = [
                    'sfid', 'margin3count', 'margin5count', 'margin10count', 'margin20count', 'amount'
                ]
                # Prepare the INSERT query with ON CONFLICT to avoid duplicate entries
                coinnumber_query = f"""
                    INSERT INTO Coinnumber ({', '.join(coinnumber_columns)})
                    VALUES ({', '.join(['%s'] * len(coinnumber_columns))})
                    ON CONFLICT (sfid) DO NOTHING
                """
                # Prepare the data for insertion
                coinnumber_values = [
                    (
                        obj['sfid'],
                        obj['margin3count'],
                        obj['margin5count'],
                        obj['margin10count'],
                        obj['margin20count'],
                        obj['amount']
                    )
                    for obj in coinnumber_data
                ]
                # Execute batch insertion
                execute_batch(cursor, coinnumber_query, coinnumber_values, page_size=100)
                logging.info(f"Inserted {len(coinnumber_values)} records into 'Coinnumber' table successfully.")
    except Exception as error:
        logging.error(f"Error inserting data into 'Coinnumber' table: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
    while True:
        logging.info("Starting data reset process.")
        truncate_tables()  # Clear existing data
        create_tables()    # Ensure tables are created

        # Insert Coinnumber data
        coinnumber_data = get_coinnumber_data()
        insert_coinnumber_data(coinnumber_data)

        # Fetch trading data from Binance and insert into trading table
        trading_data = getall_data()  # Fetch trading data
        insert_data_db(trading_data)  # Insert data into PostgreSQL tables

        logging.info("Data reset process completed. Sleeping for 24 hours.")
        time.sleep(86400)  # Sleep for 24 hours

if __name__ == "__main__":
    main()
