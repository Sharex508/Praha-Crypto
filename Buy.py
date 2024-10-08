import time
import psycopg2
import requests
import logging

logging.basicConfig(level=logging.DEBUG)

def get_db_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="HarshaCry")
    return connection, connection.cursor()

def get_data_for_santos():
    """Fetch the SANTOSUSDT data from the Binance API."""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    santos_data = next((d for d in data if d['symbol'] == 'SANTOSUSDT'), None)
    if santos_data:
        logging.debug(f"DEBUG - Fetched SANTOSUSDT from API with price: {santos_data['price']}")
    else:
        logging.debug("DEBUG - SANTOSUSDT not found in API data.")
    return [santos_data] if santos_data else []

def get_santos_result():
    """Retrieve the SANTOSUSDT trading data from the database."""
    connection, cursor = get_db_connection()
    try:
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
               mar3, mar5, mar10, mar20
        FROM trading
        WHERE symbol = 'SANTOSUSDT'
        """
        cursor.execute(sql)
        result = cursor.fetchone()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'mar3', 'mar5', 'mar10', 'mar20')
        santos_data = dict(zip(keys, result)) if result else None
        logging.debug(f"DEBUG - Fetched SANTOSUSDT record from database: {santos_data}")
        return [santos_data] if santos_data else []
    except Exception as e:
        logging.error(f"Error fetching SANTOSUSDT: {e}")
    finally:
        cursor.close()
        connection.close()

def get_coin_limits():
    """Retrieve margin count limits and amount for purchases from the Coinnumber table."""
    connection, cursor = get_db_connection()
    try:
        sql = "SELECT margin3count, Margin5count, Margin10count, Margin20count, amount FROM Coinnumber"
        cursor.execute(sql)
        limits = cursor.fetchone()
        coin_limits = {
            "margin3count": int(float(limits[0])),
            "margin5count": int(float(limits[1])),
            "margin10count": int(float(limits[2])),
            "margin20count": int(float(limits[3])),
            "amount": float(limits[4])
        }
        logging.debug(f"DEBUG - Coin Limits: {coin_limits}")
        return coin_limits
    except Exception as e:
        logging.error(f"Error fetching coin limits: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def calculate_margin_level(db_match_data, coin_limits):
    mar3_purchased = db_match_data['mar3']
    mar5_purchased = db_match_data['mar5']
    mar10_purchased = db_match_data['mar10']
    mar20_purchased = db_match_data['mar20']

    logging.debug(
        f"DEBUG - Purchased Flags for SANTOSUSDT:\n"
        f"mar3: {mar3_purchased}, mar5: {mar5_purchased}, "
        f"mar10: {mar10_purchased}, mar20: {mar20_purchased}\n"
        f"Coin Limits: {coin_limits}"
    )

    if not mar20_purchased and coin_limits['margin20count'] > 0:
        return float(db_match_data['margin20']), 'mar20'
    elif not mar10_purchased and coin_limits['margin10count'] > 0:
        return float(db_match_data['margin10']), 'mar10'
    elif not mar5_purchased and coin_limits['margin5count'] > 0:
        return float(db_match_data['margin5']), 'mar5'
    elif not mar3_purchased and coin_limits['margin3count'] > 0:
        return float(db_match_data['margin3']), 'mar3'
    
    return None, None

def task_santos(db_resp, api_resp):
    """Process only the SANTOSUSDT symbol."""
    coin_limits = get_coin_limits()
    if not coin_limits:
        logging.error("Error: Coin limits not found.")
        return

    db_match_data = db_resp[0] if db_resp else None
    api_match_data = api_resp[0] if api_resp else None
    
    if not db_match_data:
        logging.debug("DEBUG - No database record for SANTOSUSDT.")
        return
    
    if not api_match_data:
        logging.debug("DEBUG - No API data for SANTOSUSDT.")
        return
    
    api_last_price = float(api_match_data['price'])
    logging.debug(f"DEBUG - Processing SANTOSUSDT with API last price: {api_last_price}")
    
    db_margin, margin_level = calculate_margin_level(db_match_data, coin_limits)
    logging.debug(f"DEBUG - Margin Check for SANTOSUSDT: Level - {margin_level}, Required Price - {db_margin}")

    if db_margin and api_last_price >= db_margin:
        amount = coin_limits['amount']
        logging.debug(
            f"Action: Buying SANTOSUSDT\n"
            f"Level: {margin_level}\n"
            f"Required Margin: {db_margin}\n"
            f"Amount: {amount}"
        )
        # Call update_margin_status if required for actual database update
    else:
        logging.debug(
            f"No action for SANTOSUSDT.\n"
            f"Current Price: {api_last_price}\n"
            f"Margin Level: {db_margin or 'N/A'}"
        )

def show_santos():
    """Run a single iteration for SANTOSUSDT."""
    logging.debug(f"Starting new iteration for SANTOSUSDT at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    api_resp = get_data_for_santos()
    db_resp = get_santos_result()
    task_santos(db_resp, api_resp)

if __name__ == "__main__":
    show_santos()
