import time
import logging
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_batch
import requests
from simple_salesforce import Salesforce

logging.basicConfig(level=logging.INFO)

def get_database_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry",
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
    """Create the trading and Coinnumber tables with correct data types."""
    try:
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                # Create trading table with FLOAT data types for margin columns
                create_trading_table_query = '''
                    CREATE TABLE IF NOT EXISTS trading (
                        symbol          TEXT    NOT NULL,
                        intialPrice     FLOAT,
                        highPrice       FLOAT,
                        lastPrice       FLOAT,
                        margin3         FLOAT,
                        margin5         FLOAT,
                        margin10        FLOAT,
                        margin20        FLOAT,
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
                logging.info("Table 'trading' created with FLOAT types for margins.")

                # Create Coinnumber table with sfid as primary key
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
    """Fetch trading data from Binance API and calculate margin values."""
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
            'purchasePrice': None  # Set as None to indicate no initial purchase price
        }
        for d in data if filter in d['symbol']
    ]
    logging.info(f"Fetched and processed {len(trading_data)} records from Binance API.")
    return trading_data

def fetch_coinnumber_data_from_salesforce():
    """Fetch margin and amount data from Salesforce Account records."""
    # Replace with your Salesforce credentials
    sf = Salesforce(username='your_username', password='your_password', security_token='your_security_token')
    query = "SELECT ID, Margin3Count__c, Margin5Count__c, Margin10Count__c, Margin20Count__c, Amount__c FROM Account"
    records = sf.query_all(query)

    # Transform Salesforce records into the required format
    coinnumber_data = [
        (
            record['Id'],  # Salesforce ID mapped to sfid
            int(record.get('Margin3Count__c', '0')),
            int(record.get('Margin5Count__c', '0')),
            int(record.get('Margin10Count__c', '0')),
            int(record.get('Margin20Count__c', '0')),
            float(record.get('Amount__c', '5'))  # Default to 5 if no value provided
        )
        for record in records['records']
    ]
    logging.info(f"Fetched {len(coinnumber_data)} Coinnumber records from Salesforce.")
    return coinnumber_data

def insert_data_db(trading_data, coinnumber_data):
    """Insert trading and Coinnumber data into the PostgreSQL database."""
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

                # Insert into Coinnumber table
                coinnumber_query = '''
                    INSERT INTO Coinnumber (sfid, margin3count, margin5count, margin10count, margin20count, amount)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sfid) DO NOTHING;
                '''
                execute_batch(cursor, coinnumber_query, coinnumber_data)
                logging.info("Inserted Coinnumber data into 'Coinnumber' table.")
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
