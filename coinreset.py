import time
import psycopg2
from psycopg2 import Error
import requests
import logging
from psycopg2.extras import execute_batch  # Import extras module

logging.basicConfig(level=logging.INFO)

def get_database_connection():
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry",
    )

def table_Delete_crypto():
    try:
        with get_database_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM trading")
                logging.info("Rows deleted successfully...")
    except psycopg2.Error as e:
        logging.error(f"Error deleting rows: {e}")

def table_Create_crypto():
    try:
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                create_table_query = '''
                    CREATE TABLE IF NOT EXISTS trading
                    (
                    symbol            TEXT    NOT NULL,
                    intialPrice       TEXT,
                    highPrice         TEXT,
                    lastPrice         TEXT,
                    margin3           TEXT,
                    margin5           TEXT,
                    margin10          TEXT,
                    margin20          TEXT,
                    purchasePrice     TEXT,
                    margin3count      TEXT,
                    Margin5count      TEXT,
                    Margin10count     TEXT,
                    Margin20count     TEXT,
                    mar3              BOOLEAN DEFAULT FALSE,  
                    mar5              BOOLEAN DEFAULT FALSE,  
                    mar10             BOOLEAN DEFAULT FALSE,  
                    mar20             BOOLEAN DEFAULT FALSE,  
                    created_at        TEXT,
                    status            TEXT  DEFAULT '0'
                    );
                '''
                cursor.execute(create_table_query)
                logging.info("Table created successfully in PostgreSQL - trading")
    except (Exception, Error) as error:
        logging.error(f"Error while creating table in PostgreSQL: {error}")

def getall_data(filter='USDT'):
    # Fetch data from Binance API
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()

    # Filter symbols based on the filter and ensure price exists
    resp = [d for d in data if filter in d['symbol'] and 'price' in d]

    for obj in resp:
        lprice = float(obj['price'])  # This is the 'price' from Binance
        marg = lprice * 1.03
        marg1 = lprice * 1.05
        marg2 = lprice * 1.10
        marg3 = lprice * 1.20

        # Map the 'price' from Binance API to the appropriate columns
        obj.update({
            "initialPrice": lprice,      # Use this to insert into the initialPrice column
            "highPrice": lprice,         # Use this to insert into the highPrice column
            "lastPrice": lprice,         # Use this to insert into the lastPrice column
            "margin3": marg,
            "margin5": marg1,
            "margin10": marg2,
            "margin20": marg3,
            "purchasePrice": ""          # No purchase price yet
        })
        logging.info('Completed processing data for %s', obj['symbol'])

    return resp

def insert_data_db(resp):
    try:
        with get_database_connection() as connection:
            connection.autocommit = True

            with connection.cursor() as cursor:
                # Define the columns in the trading table where data will be inserted
                columns = ['symbol', 'initialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 'purchasePrice']
                placeholders = ','.join(['%s'] * len(columns))
                query = f"INSERT INTO trading ({','.join(columns)}) VALUES ({placeholders})"

                # Prepare the data tuples for insertion
                values = [
                    [obj['symbol'], obj['initialPrice'], obj['highPrice'], obj['lastPrice'], obj['margin3'], obj['margin5'], obj['margin10'], obj['margin20'], obj['purchasePrice']]
                    for obj in resp
                ]
                tuples = [tuple(x) for x in values]

                # Execute batch insert
                execute_batch(cursor, query, tuples)
                logging.info("Data inserted successfully in trading table.")

    except Exception as error:
        logging.error(f"Error while inserting data into PostgreSQL: {error}")

def main():
    while True:
        table_Delete_crypto()  # Delete old data
        table_Create_crypto()  # Create table if not exists
        data = getall_data()  # Fetch data from Binance API
        insert_data_db(data)  # Insert the data into PostgreSQL
        time.sleep(86400)  # Sleep for 24 hours

if __name__ == "__main__":
    main()
