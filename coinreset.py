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
                logging.info("Table created successfully in PostgreSQL - trading_test")
    except (Exception, Error) as error:
        logging.error(f"Error while connecting to PostgreSQL: {error}")


def getall_data(filter='USDT'):
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()

    resp = [d for d in data if filter in d['symbol'] and 'price' in d]

    for obj in resp:
        lprice = float(obj['price'])
        marg = lprice * 1.03
        marg1 = lprice * 1.05
        marg2 = lprice * 1.10
        marg3 = lprice * 1.20

        obj.update({
            "initialPrice": lprice,
            "highPrice": lprice,
            "margin3": marg,
            "margin5": marg1,
            "margin10": marg2,
            "margin20": marg3,
            "purchasePrice": ""
        })
        logging.info('completed')

    return resp


def insert_data_db(resp):
    try:
        with get_database_connection() as connection:
            connection.autocommit = True

            with connection.cursor() as cursor:
                # Get column names from the first element of `resp`
                columns = ','.join(resp[0].keys())
                # Prepare query string with all columns
                query = f"INSERT INTO trading ({columns}) VALUES %s"

                # Prepare the data tuples for insertion
                values = [
                    [value.strip() if isinstance(value, str) else value for value in obj.values()]
                    for obj in resp
                ]
                tuples = [tuple(x) for x in values]

                # Execute batch insert
                psycopg2.extras.execute_batch(cursor, query, tuples)
                logging.info("Data inserted successfully in trading table.")

    except Exception as error:
        logging.error(f"Error while inserting data into PostgreSQL: {error}")


def main():
    while True:
        table_Delete_crypto()
        table_Create_crypto()
        data = getall_data()
        insert_data_db(data)
        time.sleep(86400)

if __name__ == "__main__":
    main()