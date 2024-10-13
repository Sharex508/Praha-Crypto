import psycopg2
from psycopg2 import sql
import logging


def get_database_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry"
    )

def fetch_purchased_coins():
    """Fetch purchased coins from the trading table."""
    connection = get_database_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice, mar3, mar5, mar10, mar20, created_at, status
                    FROM trading
                    WHERE status = '1'
                """)
                cursor.execute(query)
                coins = cursor.fetchall()
                # Column names for table display
                columns = [desc[0] for desc in cursor.description]
                return columns, coins
    except Exception as e:
        print(f"Error fetching purchased coins: {e}")
    finally:
        connection.close()

def display_table(columns, rows):
    """Display data in a formatted table."""
    # Define column widths
    col_widths = [max(len(str(row[i])) for row in rows + [columns]) + 2 for i in range(len(columns))]

    # Print header
    header = "".join(str(columns[i]).ljust(col_widths[i]) for i in range(len(columns)))
    print(header)
    print("-" * len(header))

    # Print rows
    for row in rows:
        print("".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row))))

def reset_trading_table():
    """Reset purchase-related fields in the trading table."""
    connection = get_database_connection()
    cursor = connection.cursor()
    try:
        # SQL to reset specific fields in the trading table
        sql = """
        UPDATE trading
        SET 
            purchasePrice = NULL,
            mar3 = FALSE,
            mar5 = FALSE,
            mar10 = FALSE,
            mar20 = FALSE,
            created_at = NULL,
            status = '0'
        """
        cursor.execute(sql)
        connection.commit()
        logging.info("Trading table purchase-related fields have been reset successfully.")
        print("Trading table purchase-related fields have been reset successfully.")

    except Exception as e:
        logging.error(f"Error resetting trading table: {e}")
    finally:
        cursor.close()
        connection.close()


def main():
    #reset_trading_table()

    columns, purchased_coins = fetch_purchased_coins()
    if purchased_coins:
        display_table(columns, purchased_coins)
    else:
       print("No purchased coins found.")

if __name__ == "__main__":
    main()
