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

def get_data_from_wazirx(filter='USDT'):
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    filtered_data = [d for d in data if filter in d['symbol'] and 'price' in d]
    logging.debug(f"DEBUG - Fetched {len(filtered_data)} symbols from API with filter '{filter}'.")
    return filtered_data

def get_results():
    connection, cursor = get_db_connection()
    try:
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
               mar3, mar5, mar10, mar20
        FROM trading
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'mar3', 'mar5', 'mar10', 'mar20')
        data = [dict(zip(keys, obj)) for obj in results]
        logging.debug(f"DEBUG - Fetched {len(data)} trading records from the database.")
        return data
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
    finally:
        cursor.close()
        connection.close()

def get_coin_limits():
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
    mar3_purchased = sum(1 for coin in db_match_data if coin['mar3'] == True)
    mar5_purchased = sum(1 for coin in db_match_data if coin['mar5'] == True)
    mar10_purchased = sum(1 for coin in db_match_data if coin['mar10'] == True)
    mar20_purchased = sum(1 for coin in db_match_data if coin['mar20'] == True)

    logging.debug(
        f"DEBUG - Purchased Counts:\n"
        f"mar3_purchased: {mar3_purchased}, mar5_purchased: {mar5_purchased}, "
        f"mar10_purchased: {mar10_purchased}, mar20_purchased: {mar20_purchased}\n"
        f"Coin Limits: {coin_limits}"
    )

    if mar20_purchased < coin_limits['margin20count'] and not db_match_data['mar20']:
        return float(db_match_data['margin20']), 'mar20'
    elif mar10_purchased < coin_limits['margin10count'] and not db_match_data['mar10']:
        return float(db_match_data['margin10']), 'mar10'
    elif mar5_purchased < coin_limits['margin5count'] and not db_match_data['mar5']:
        return float(db_match_data['margin5']), 'mar5'
    elif mar3_purchased < coin_limits['margin3count'] and not db_match_data['mar3']:
        return float(db_match_data['margin3']), 'mar3'
    
    return None, None

def task(db_resp, api_resp):
    coin_limits = get_coin_limits()
    if not coin_limits:
        logging.error("Error: Coin limits not found.")
        return

    for ele in db_resp:
        symbol = ele["symbol"]
        db_match_data = ele
        api_match_data = next((item for item in api_resp if item["symbol"] == symbol), None)
        
        if not api_match_data:
            logging.debug(f"DEBUG - Symbol {symbol} not found in API data.")
            continue

        api_last_price = float(api_match_data['price'])
        logging.debug(f"DEBUG - Processing {symbol} with API last price: {api_last_price}")
        
        db_margin, margin_level = calculate_margin_level(db_match_data, coin_limits)
        logging.debug(f"DEBUG - Margin Check for {symbol}: Level - {margin_level}, Required Price - {db_margin}")

        if db_margin and api_last_price >= db_margin:
            amount = coin_limits['amount']
            base_asset_symbol = symbol.replace('USDT', '')
            logging.debug(
                f"Action: Buying {base_asset_symbol}\n"
                f"Level: {margin_level}\n"
                f"Required Margin: {db_margin}\n"
                f"Amount: {amount}"
            )
            update_margin_status(db_match_data['symbol'], margin_level)
        else:
            logging.debug(
                f"No action for {symbol}.\n"
                f"Current Price: {api_last_price}\n"
                f"Margin Level: {db_margin or 'N/A'}"
            )

def update_margin_status(symbol, margin_level):
    connection, cursor = get_db_connection()
    try:
        sql_update_trading = f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s"
        cursor.execute(sql_update_trading, (symbol,))
        
        if margin_level == 'mar3':
            sql_update_coinnumber = "UPDATE Coinnumber SET margin3count = margin3count - 1"
        elif margin_level == 'mar5':
            sql_update_coinnumber = "UPDATE Coinnumber SET Margin5count = Margin5count - 1"
        elif margin_level == 'mar10':
            sql_update_coinnumber = "UPDATE Coinnumber SET Margin10count = Margin10count - 1"
        elif margin_level == 'mar20':
            sql_update_coinnumber = "UPDATE Coinnumber SET Margin20count = Margin20count - 1"
        
        cursor.execute(sql_update_coinnumber)
        connection.commit()
        logging.debug(f"{symbol} purchased at {margin_level}. Remaining count updated.")
    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def show():
    while True:
        logging.debug(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        api_resp = get_data_from_wazirx()
        db_resp = get_results()
        task(db_resp, api_resp)  # Direct call to task without threading
        time.sleep(5)

if __name__ == "__main__":
    show()
