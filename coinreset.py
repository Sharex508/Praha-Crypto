import time
import logging
import psycopg2
from psycopg2.extras import execute_batch
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

def create_coinnumber_table():
    """Create the Coinnumber table if it doesn't exist."""
    connection = get_database_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                create_table_query = '''
                    CREATE TABLE IF NOT EXISTS Coinnumber (
                        sfid            TEXT PRIMARY KEY,
                        margin3count    TEXT,
                        margin5count    TEXT,
                        margin10count   TEXT,
                        margin20count   TEXT,
                        amount          TEXT
                    );
                '''
                cursor.execute(create_table_query)
                logging.info("Table 'Coinnumber' created successfully.")
    except Exception as error:
        logging.error(f"Error creating 'Coinnumber' table: {error}")
    finally:
        connection.close()

def fetch_coinnumber_data_from_salesforce():
    """Fetch margin and amount data from Salesforce Account records."""
    sf = Salesforce(username='your_username', password='your_password', security_token='your_security_token')
    query = "SELECT Id, margin_3__c, margin_5__c, margin_10__c, margin_20__c, Amount__c FROM Account"
    records = sf.query_all(query)

    coinnumber_data = []
    for record in records['records']:
        coinnumber_data.append((
            record['Id'],                           # Mapped to 'sfid'
            record.get('margin_3__c', '0'),         # Mapped to 'margin3count'
            record.get('margin_5__c', '0'),         # Mapped to 'margin5count'
            record.get('margin_10__c', '0'),        # Mapped to 'margin10count'
            record.get('margin_20__c', '0'),        # Mapped to 'margin20count'
            record.get('Amount__c', '5')            # Mapped to 'amount', default to 5
        ))
    logging.info("Fetched Coinnumber data from Salesforce.")
    return coinnumber_data

def insert_coinnumber_data(data):
    """Insert fetched Salesforce data into the Coinnumber table."""
    connection = get_database_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                insert_query = '''
                    INSERT INTO Coinnumber (sfid, margin3count, margin5count, margin10count, margin20count, amount)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sfid) DO NOTHING;
                '''
                execute_batch(cursor, insert_query, data)
                logging.info("Data inserted successfully into 'Coinnumber' table.")
    except Exception as error:
        logging.error(f"Error inserting data into 'Coinnumber' table: {error}")
    finally:
        connection.close()

def main():
    create_coinnumber_table()  # Create Coinnumber table if it doesn't exist
    coinnumber_data = fetch_coinnumber_data_from_salesforce()  # Fetch data from Salesforce
    insert_coinnumber_data(coinnumber_data)  # Insert the data into PostgreSQL

if __name__ == "__main__":
    main()
