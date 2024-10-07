import time
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor
from notifications import notisend  # Import your existing notification module
import logging

logging.basicConfig(level=logging.INFO)

def get_db_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="HarshaCry")
    return connection, connection.cursor()

def get_data_from_wazirx(filter='USDT'):
    """Fetch the current price data from the Binance API."""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    return [d for d in data if filter in d['symbol'] and 'price' in d]

def get_results():
    """Retrieve trading data from the database."""
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
        return data
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
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
        return {
            "margin3count": int(float(limits[0])),
            "margin5count": int(float(limits[1])),
            "margin10count": int(float(limits[2])),
            "margin20count": int(float(limits[3])),
            "amount": float(limits[4])
        }
    except Exception as e:
        logging.error(f"Error fetching coin limits: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def get_diff_of_db_api_values(api_resp):
    """Compare database values with the Binance API data."""
    db_resp = get_results()
    dicts_data = [obj['symbol'] for obj in db_resp]
    chunk_size = min(20, len(dicts_data))
    chunks = [dicts_data[i:i + chunk_size] for i in range(0, len(dicts_data), chunk_size)]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        for chunk in chunks:
            executor.submit(task, db_resp, api_resp, chunk)

def task(db_resp, api_resp, data):
    """Perform comparison task for each chunk of symbols."""
    coin_limits = get_coin_limits()
    if not coin_limits:
        notisend("Error: Coin limits not found.")
        return

    for ele in data:
        db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
        if not db_match_data:
            continue
        api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
        if not api_match_data:
            continue
        
        api_last_price = float(api_match_data['lastPrice'])
        db_margin, margin_level = calculate_margin_level(db_match_data, coin_limits)
        
        if db_margin and api_last_price >= db_margin:
            amount = coin_limits['amount']
            base_asset_symbol = ele.replace('USDT', '')
            notisend(
                f"Action: Buying {base_asset_symbol}\n"
                f"Level: {margin_level}\n"
                f"Required Margin: {db_margin}\n"
                f"Amount: {amount}"
            )
            update_margin_status(db_match_data['symbol'], margin_level)
        else:
            notisend(
                f"No action for {ele}.\n"
                f"Current Price: {api_last_price}\n"
                f"Margin Level: {db_margin or 'N/A'}"
            )

def calculate_margin_level(db_match_data, coin_limits):
    """Determine which margin level to apply based on purchase limits."""
    
    mar3_purchased = sum(1 for coin in db_match_data if coin['mar3'] == True)
    mar5_purchased = sum(1 for coin in db_match_data if coin['mar5'] == True)
    mar10_purchased = sum(1 for coin in db_match_data if coin['mar10'] == True)
    mar20_purchased = sum(1 for coin in db_match_data if coin['mar20'] == True)

    if mar3_purchased < coin_limits['margin3count'] and not db_match_data['mar3']:
        return float(db_match_data['margin3']), 'mar3'
    elif mar5_purchased < coin_limits['margin5count'] and not db_match_data['mar5']:
        return float(db_match_data['margin5']), 'mar5'
    elif mar10_purchased < coin_limits['margin10count'] and not db_match_data['mar10']:
        return float(db_match_data['margin10']), 'mar10'
    elif mar20_purchased < coin_limits['margin20count'] and not db_match_data['mar20']:
        return float(db_match_data['margin20']), 'mar20'
    
    return None, None

def update_margin_status(symbol, margin_level):
    """Update the margin status in the database and decrement the count in Coinnumber."""
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
        notisend(f"{symbol} purchased at {margin_level}. Remaining count updated.")
    except Exception as e:
        notisend(f"Error updating margin status for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_last_prices(api_resp):
    """Update last prices in the database."""
    db_resp = get_active_trades()
    updates = []
    for trade in db_resp:
        symbol = trade['symbol']
        matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
        if matching_api_data:
            new_last_price = matching_api_data['lastPrice']
            updates.append((new_last_price, symbol))
    
    update_coin_last_price_batch(updates)

def update_coin_last_price_batch(updates):
    """Batch update last prices in the trading table."""
    if not updates:
        return
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET lastPrice = %s WHERE symbol = %s"
        cursor.executemany(sql, updates)
        connection.commit()
        notisend(f"Updated last prices for {len(updates)} symbols")
    except Exception as e:
        notisend(f"Error updating last prices: {e}")
    finally:
        cursor.close()
        connection.close()

def get_active_trades():
    """Retrieve active trades from the database."""
    connection, cursor = get_db_connection()
    try:
        sql = "SELECT * FROM trading WHERE status = '1'"
        cursor.execute(sql)
        results = cursor.fetchall()
        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 
                'margin10', 'margin20', 'purchasePrice', 'quantity', 'created_at', 'status')
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        notisend(f"Error fetching active trades: {e}")
    finally:
        cursor.close()
        connection.close()

def show():
    start_time = time.time()
    while True:
        try:
            notisend(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            api_resp = get_data_from_wazirx()
            get_diff_of_db_api_values(api_resp)
            update_last_prices(api_resp)
            
            end_time = time.time()
            notisend(f"Iteration completed in {end_time - start_time:.2f} seconds")
            time.sleep(5)
        except Exception as e:
            notisend(f"An error occurred: {e}")
            time.sleep(5)

if __name__ == "__main__":
    show()
