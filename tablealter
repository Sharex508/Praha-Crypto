import psycopg2
import logging

def get_database_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry",
    )

def update_margin_values():
    """Update margin3, margin5, margin10, margin20 values in the trading table."""
    try:
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                # Fetch all symbols and their initial prices
                cursor.execute("SELECT symbol, intialPrice FROM trading")
                records = cursor.fetchall()
                
                for symbol, intial_price in records:
                    try:
                        if intial_price is None:
                            logging.warning(f"Skipping {symbol}: intialPrice is None")
                            continue

                        price = float(intial_price)
                        margin3 = price * 1.03
                        margin5 = price * 1.05
                        margin10 = price * 1.10
                        margin20 = price * 1.20
                        
                        # Update the margin values for this symbol
                        cursor.execute("""
                            UPDATE trading
                            SET margin3 = %s,
                                margin5 = %s,
                                margin10 = %s,
                                margin20 = %s
                            WHERE symbol = %s
                        """, (margin3, margin5, margin10, margin20, symbol))
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Skipping {symbol} due to invalid intialPrice: {intial_price}")
                
                connection.commit()
                logging.info("Margin values updated successfully.")
    except Exception as e:
        logging.error(f"Error updating margin values: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    update_margin_values()
