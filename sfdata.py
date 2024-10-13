import psycopg2
import logging

logging.basicConfig(level=logging.INFO)

def alter_coinnumber_table():
    """Convert decimal strings to integers and alter the Coinnumber table columns to INTEGER type."""
    connection = None
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="Harsha508",
            host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
            port="5432",
            database="HarshaCry"
        )
        
        with connection:
            with connection.cursor() as cursor:
                # Step 1: Update values to remove decimal points
                update_query = '''
                UPDATE Coinnumber
                SET
                    margin3count = TRIM(TRAILING '.0' FROM margin3count),
                    margin5count = TRIM(TRAILING '.0' FROM margin5count),
                    margin10count = TRIM(TRAILING '.0' FROM margin10count),
                    margin20count = TRIM(TRAILING '.0' FROM margin20count),
                    amount = TRIM(TRAILING '.0' FROM amount);
                '''
                cursor.execute(update_query)
                logging.info("Decimal values converted to integer-compatible format.")

                # Step 2: Alter columns to INTEGER type
                alter_table_query = '''
                ALTER TABLE Coinnumber
                    ALTER COLUMN margin3count TYPE INTEGER USING margin3count::INTEGER,
                    ALTER COLUMN margin5count TYPE INTEGER USING margin5count::INTEGER,
                    ALTER COLUMN margin10count TYPE INTEGER USING margin10count::INTEGER,
                    ALTER COLUMN margin20count TYPE INTEGER USING margin20count::INTEGER,
                    ALTER COLUMN amount TYPE INTEGER USING amount::INTEGER;
                '''
                cursor.execute(alter_table_query)
                logging.info("Table 'Coinnumber' columns altered to INTEGER type successfully.")
    except Exception as error:
        logging.error(f"Error altering 'Coinnumber' table: {error}")
    finally:
        if connection:
            connection.close()

# Run the function
alter_coinnumber_table()
