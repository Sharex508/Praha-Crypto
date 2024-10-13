import psycopg2
from psycopg2 import sql
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

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
        return

    try:
        with connection:
            with connection.cursor() as cursor:
                # Construct and execute query to fetch all rows from the specified table
                query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                cursor.execute(query)
                
                # Fetch and format column names and row data
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # Display data in a table format
                display_table(columns, rows)
                
                logging.info(f"Fetched {len(rows)} records from table '{table_name}'")
                
    except Exception as e:
        logging.error(f"Error fetching data from table {table_name}: {e}")
    finally:
        connection.close()
        logging.info("Database connection closed.")

def display_table(columns, rows):
    """Display data in a formatted table."""
    col_widths = [max(len(str(row[i])) for row in rows + [columns]) + 2 for i in range(len(columns))]
    header = "".join(str(columns[i]).ljust(col_widths[i]) for i in range(len(columns)))
    
    print(header)
    print("-" * len(header))
    for row in rows:
        print("".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row))))

if __name__ == "__main__":
    # Specify the name of the table you want to retrieve data from
    table_name = "trading"  # Change this to any table name in your database
    fetch_all_data_from_table(table_name)
