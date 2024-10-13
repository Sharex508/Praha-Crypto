import logging
import psycopg2
from psycopg2.extras import execute_batch
from simple_salesforce import Salesforce

logging.basicConfig(level=logging.INFO)

# Salesforce connection details
def fetch_coinnumber_data_from_salesforce():
    # Replace with your Salesforce credentials
    sf = Salesforce(username='harshacrypto508@crypto.com', password='Harsha508@2024', security_token='yPGnaLPAjlnpZmLWSeu8YCNB')
    query = "SELECT Id, margin_3__c, margin_5__c, margin_10__c, margin_20__c, Amount__c FROM Account"
    
    # Run the query
    records = sf.query_all(query)
    
    # Transform Salesforce records into the required format
    coinnumber_data = []
    for record in records['records']:
        coinnumber_data.append((
            record['Id'],  # Salesforce ID mapped to sfid
            record.get('margin_3__c', '0'),
            record.get('margin_5__c', '0'),
            record.get('margin_10__c', '0'),
            record.get('margin_20__c', '0'),
            record.get('Amount__c', '5')  # Default to 5 if no value provided
        ))
    
    logging.info("Fetched Coinnumber data from Salesforce.")
    return coinnumber_data

# PostgreSQL connection details
def get_database_connection():
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry",
    )

# Create Coinnumber table
def create_coinnumber_table():
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
        logging.error(f"Error creating Coinnumber table: {error}")
    finally:
        connection.close()

# Insert data into Coinnumber table
def insert_coinnumber_data(data):
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
        logging.error(f"Error inserting data into Coinnumber table: {error}")
    finally:
        connection.close()

def main():
    # Step 1: Create the Coinnumber table
    create_coinnumber_table()

    # Step 2: Fetch data from Salesforce
    coinnumber_data = fetch_coinnumber_data_from_salesforce()

    # Step 3: Insert the fetched data into the Coinnumber table
    insert_coinnumber_data(coinnumber_data)

if __name__ == "__main__":
    main()
