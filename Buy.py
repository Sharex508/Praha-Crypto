import time
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor

def get_db_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="HarshaCry")
    return connection, connection.cursor()

def get_data_from_wazirx(filter='USDT'):
    """Fetch the current price data from the Binance API"""
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    return [d for d in data if filter in d['symbol'] and 'price' in d]

def get_results():
    """Retrieve trading data from the database."""
    connection, cursor = get_db_connection()
    try:
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
               margin3count, Margin5count, Margin10count, Margin20count, mar3, mar5, mar10, mar20
        FROM trading
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'margin3count', 'Margin5count', 'Margin10count', 'Margin20count', 
                'mar3', 'mar5', 'mar10', 'mar20')
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        print(f"Error fetching results: {e}")
    finally:
        cursor.close()
        connection.close()

def get_diff_of_db_api_values(api_resp):
    """Compare database values with the Binance API data."""
    db_resp = get_results()
    dicts_data = [obj['symbol'] for obj in db_resp]
    chunk_size = min(20, len(dicts_data))  # Set chunk size small to avoid overwhelming threads
    chunks = [dicts_data[i:i + chunk_size] for i in range(0, len(dicts_data), chunk_size)]
    
    # Use ThreadPoolExecutor to parallelize tasks
    with ThreadPoolExecutor(max_workers=4) as executor:
        for chunk in chunks:
            executor.submit(task, db_resp, api_resp, chunk)

def task(db_resp, api_resp, data):
    """Perform comparison task for each chunk of symbols."""
    print("entered into task")
    for ele in data:
        db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
        if not db_match_data:
            continue
        api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
        if not api_match_data:
            continue
        
        api_last_price = float(api_match_data['lastPrice'])
        db_margin, margin_level = calculate_margin_level(db_match_data)
        
        if db_margin and api_last_price >= db_margin:
            usd_amount = 5
            base_asset_symbol = ele.replace('USDT', '')
            print(f"Buying {base_asset_symbol} at {db_margin} for margin level {margin_level}")
            update_margin_status(db_match_data['symbol'], margin_level)
            print(f"Successfully purchased {base_asset_symbol} at {api_last_price}")
        else:
            print(f"No action taken for {ele}. Current price: {api_last_price}, margin level: {db_margin}")

def calculate_margin_level(db_match_data):
    """Determine which margin level to apply."""
    mar3_purchased = db_match_data['mar3']
    mar5_purchased = db_match_data['mar5']
    mar10_purchased = db_match_data['mar10']
    mar20_purchased = db_match_data['mar20']

    margin3count = int(db_match_data['margin3count'])
    margin5count = int(db_match_data['Margin5count'])
    margin10count = int(db_match_data['Margin10count'])
    margin20count = int(db_match_data['Margin20count'])

    if mar3_purchased < margin3count:
        return float(db_match_data['margin3']), 'mar3'
    elif mar5_purchased < margin5count:
        return float(db_match_data['margin5']), 'mar5'
    elif mar10_purchased < margin10count:
        return float(db_match_data['margin10']), 'mar10'
    elif mar20_purchased < margin20count:
        return float(db_match_data['margin20']), 'mar20'
    return None, None

def update_margin_status(symbol, margin_level):
    """Update the margin status in the database."""
    connection, cursor = get_db_connection()
    try:
        sql = f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s"
        cursor.execute(sql, (symbol,))
        connection.commit()
        print(f"Updated {symbol} to {margin_level}")
    except Exception as e:
        print(f"Error updating margin status: {e}")
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
        print(f"Updated last prices for {len(updates)} symbols")
    except Exception as e:
        print(f"Error updating last prices: {e}")
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
        print(f"Error fetching active trades: {e}")
    finally:
        cursor.close()
        connection.close()

def show():
    start_time = time.time()  # Track the start time for performance analysis
    while True:
        try:
            print(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            api_resp = get_data_from_wazirx()  # Step 1: Fetch API data
            get_diff_of_db_api_values(api_resp)  # Step 2: Compare and handle differences
            update_last_prices(api_resp)  # Step 3: Update last prices
            
            end_time = time.time()
            print(f"Iteration completed in {end_time - start_time:.2f} seconds")
            time.sleep(5)  # Reduce sleep time to 5 seconds
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)

if __name__ == "__main__":
    show()
