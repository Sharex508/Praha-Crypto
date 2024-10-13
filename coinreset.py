import time
import psycopg2
from psycopg2 import Error
import requests
import logging
from psycopg2.extras import execute_batch
from simple_salesforce import Salesforce

logging.basicConfig(level=logging.INFO)

def get_database_connection():
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry",
    )

def truncate_tables():
    try:
        with get_database_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE trading")
                cursor.execute("TRUNCATE TABLE Coinnumber")
                logging.info("Tables 'trading' and 'Coinnumber' truncated successfully.")
    except psycopg2.Error as e:
        logging.error(f"Error truncating tables: {e}")

def create_tables():
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
                        status          TEXT DEFAULT '0'
                    );
                '''
                cursor.execute(create_trading_table_query)
                logging.info("Table 'trading' created successfully.")

                # Create Coinnumber table with sfid as primary key
                create_coinnumber_table_query = '''
                    CREATE TABLE IF NOT EXISTS Coinnumber (
                        sfid            TEXT    PRIMARY KEY,
                        margin3count    TEXT,
                        margin5count    TEXT,
                        margin10count   TEXT,
                        margin20count   TEXT,
                        amount          TEXT
                    );
                '''
                cursor.execute(create_coinnumber_table_query)
                logging.info("Table 'Coinnumber' created successfully.")
    except (Exception, Error) as error:
        logging.error(f"Error creating tables: {error}")

def getall_data(filter='USDT'):
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    resp = [d for d in data if filter in d['symbol'] and 'price' in d]

    for obj in resp:
        lprice = float(obj['price'])
        obj.update({
            "intialPrice": lprice,
            "highPrice": lprice,
            "lastPrice": lprice,
            "margin3": lprice * 1.03,
            "margin5": lprice * 1.05,
            "margin10": lprice * 1.10,
            "margin20": lprice * 1.20,
            "purchasePrice": ""
        })
        logging.info('Processed data for %s', obj['symbol'])

    return resp

def fetch_coinnumber_data_from_salesforce():
    # Replace with your Salesforce credentials
    sf = Salesforce(username='harshacrypto508@crypto.com', password='Harsha508@2024', security_token='yPGnaLPAjlnpZmLWSeu8YCNB')
    query = "SELECT ID, Margin3Count__c, Margin5Count__c, Margin10Count__c, Margin20Count__c, Amount__c FROM Account"
    records = sf.query_all(query)

    # Transform Salesforce records into the required format
    coinnumber_data = []
    for record in records['records']:
        coinnumber_data.append((
            record['Id'],  # Salesforce ID mapped to sfid
            record.get('Margin3Count__c', '0'),
            record.get('Margin5Count__c', '0'),
            record.get('Margin10Count__c', '0'),
            record.get('Margin20Count__c', '0'),
            record.get('Amount__c', '5')  # Default to 5 if no value provided
        ))
    logging.info("Fetched Coinnumber data from Salesforce.")
    return coinnumber_data

def insert_data_db(trading_data, coinnumber_data):
    try:
        with get_database_connection() as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Insert into trading table
                trading_columns = ['symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 'purchasePrice']
                placeholders = ','.join(['%s'] * len(trading_columns))
                trading_query = f"INSERT INTO trading ({','.join(trading_columns)}) VALUES ({placeholders})"
                trading_values = [
                    [obj['symbol'], obj['intialPrice'], obj['highPrice'], obj['lastPrice'], obj['margin3'], obj['margin5'], obj['margin10'], obj['margin20'], obj['purchasePrice']]
                    for obj in trading_data
                ]
                execute_batch(cursor, trading_query, trading_values)
                logging.info("Inserted data into trading table.")

                # Insert into Coinnumber table
                coinnumber_query = "INSERT INTO Coinnumber (sfid, margin3count, margin5count, margin10count, margin20count, amount) VALUES (%s, %s, %s, %s, %s, %s)"
                execute_batch(cursor, coinnumber_query, coinnumber_data)
                logging.info("Inserted data into Coinnumber table.")
    except Exception as error:
        logging.error(f"Error inserting data into PostgreSQL: {error}")

def main():
    while True:
        truncate_tables()  # Clear existing data
        create_tables()    # Ensure tables are created
        trading_data = getall_data()  # Fetch trading data
        coinnumber_data = fetch_coinnumber_data_from_salesforce()  # Fetch Coinnumber data from Salesforce
        insert_data_db(trading_data, coinnumber_data)  # Insert data into PostgreSQL tables
        time.sleep(86400)  # Sleep for 24 hours

if __name__ == "__main__":
    main()
