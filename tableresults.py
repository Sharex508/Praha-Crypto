import psycopg2
from psycopg2 import sql
import logging

logging.basicConfig(level=logging.DEBUG)

def get_database_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry"
    )

def fetch_status_1_records():
    """Fetch all records with status = '1' from the trading table."""
    connection = get_database_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT *
                    FROM trading
                    WHERE status = '1'
                """)
                cursor.execute(query)
                records = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                logging.debug(f"Fetched {len(records)} records with status = '1'.")
                return columns, records
    except Exception as e:
        logging.error(f"Error fetching records with status = '1': {e}")
        return None, None
    finally:
        connection.close()

def display_table(columns, rows):
    """Display data in a formatted table."""
    if not rows:
        print("No records with status = '1' found.")
        return

    col_widths = [max(len(str(row[i])) for row in rows + [columns]) + 2 for i in range(len(columns))]

    header = "".join(str(columns[i]).ljust(col_widths[i]) for i in range(len(columns)))
    print(header)
    print("-" * len(header))

    for row in rows:
        print("".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row))))

def main():
    columns, records = fetch_status_1_records()
    if records:
        display_table(columns, records)
    else:
        logging.info("No records with status = '1' found.")

if __name__ == "__main__":
    main()
