# coinreset.py

import time
import logging
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_batch
import requests

logging.basicConfig(level=logging.INFO)

def get_database_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry",      # Replace with your actual database name
    )

def truncate_tables():
    """Truncate the trading and Coinnumber tables."""
    try:
        with get_database_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE trading")
                cursor.execute("TRUNCATE TABLE Coinnumber")
                logging.info("Tables 'trading' and 'Coinnumber' truncated successfully.")
    except psycopg2.Error as e:
        logging.error(f"Error truncating tables: {e}")

def create_tables():
    """Create the trading and Coinnumber tables if they do not exist."""
    try:
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                # Create trading table
                create_trading_table_query = '''
                    CREATE TABLE IF NOT EXISTS trading (
                        symbol          TEXT    NOT NULL,
                        intialPrice     TEXT,
                        highPrice       TEXT,
                        lastPrice       TEXT,
                        margin3         TEXT,
                        margin5         TEXT,
                        margin10        TEXT,
                        margin20        TEXT,
                        purchasePrice   TEXT,
                        mar3            BOOLEAN DEFAULT FALSE,
                        mar5            BOOLEAN DEFAULT FALSE,
                        mar10           BOOLEAN DEFAULT FALSE,
                        mar20           BOOLEAN DEFAULT FALSE,
                        created_at      TEXT,
                        status          TEXT DEFAULT '0',  -- Added comma here
                        last_notified_percentage        FLOAT DEFAULT 0.0,
                        last_notified_decrease_percentage FLOAT DEFAULT 0.0
                    );
                '''
                cursor.execute(create_trading_table_query)
                logging.info("Table 'trading' created successfully.")

                # Create Coinnumber table
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
                logging.info("Table 'Coinnumber' created successfully.")
    except (Exception, Error) as error:
        logging.error(f"Error creating tables: {error}")

def getall_data(filter='USDT'):
    """Fetch trading data from Binance API."""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    trading_data = [
        {
            'symbol': d['symbol'],
            'intialPrice': float(d['price']),
            'highPrice': float(d['price']),
            'lastPrice': float(d['price']),
            'margin3': float(d['price']) * 1.03,
            'margin5': float(d['price']) * 1.05,
            'margin10': float(d['price']) * 1.10,
            'margin20': float(d['price']) * 1.20,
            'purchasePrice': ""
        }
        for d in data if filter in d['symbol']
    ]
    logging.info(f"Fetched and processed {len(trading_data)} records from Binance API.")
    return trading_data

def insert_data_db(trading_data):
    """Insert trading data into the PostgreSQL database."""
    try:
        with get_database_connection() as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Insert into trading table
                trading_columns = ['symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 'purchasePrice']
                trading_query = f"INSERT INTO trading ({','.join(trading_columns)}) VALUES ({','.join(['%s'] * len(trading_columns))})"
                trading_values = [
                    (
                        obj['symbol'], obj['intialPrice'], obj['highPrice'], obj['lastPrice'],
                        obj['margin3'], obj['margin5'], obj['margin10'], obj['margin20'], obj['purchasePrice']
                    )
                    for obj in trading_data
                ]
                execute_batch(cursor, trading_query, trading_values)
                logging.info("Inserted trading data into 'trading' table.")
    except Exception as error:
        logging.error(f"Error inserting data into PostgreSQL: {error}")

def main():
    while True:
        truncate_tables()  # Clear existing data
        create_tables()    # Ensure tables are created
        trading_data = getall_data()  # Fetch trading data
        insert_data_db(trading_data)  # Insert data into PostgreSQL tables
        time.sleep(86400)  # Sleep for 24 hours

if __name__ == "__main__":
    main()
