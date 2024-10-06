import requests
import psycopg2
from binance.spot import Spot as Client
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
import time
from assetbuy import buy_asset_with_usd
from notifications import notisend

client = Client()

def get_db_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="HarshCry")
    return connection, connection.cursor()

def get_data_from_wazirx(filter='USDT'):
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    return [d for d in data if filter in d['symbol'] and 'price' in d]

def get_results():
    connection, cursor = get_db_connection()
    try:
        sql = """
        SELECT 
            symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
            margin3count, Margin5count, Margin10count, Margin20count, mar3, mar5, mar10, mar20,
            SUM(CASE WHEN mar3 = TRUE THEN 1 ELSE 0 END) AS mar3_purchased,
            SUM(CASE WHEN mar5 = TRUE THEN 1 ELSE 0 END) AS mar5_purchased,
            SUM(CASE WHEN mar10 = TRUE THEN 1 ELSE 0 END) AS mar10_purchased,
            SUM(CASE WHEN mar20 = TRUE THEN 1 ELSE 0 END) AS mar20_purchased
        FROM trading
        GROUP BY symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
                 margin3count, Margin5count, Margin10count, Margin20count, mar3, mar5, mar10, mar20
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'margin3count', 'Margin5count', 'Margin10count', 'Margin20count', 
                'mar3', 'mar5', 'mar10', 'mar20', 'mar3_purchased', 'mar5_purchased', 'mar10_purchased', 'mar20_purchased')
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        connection.close()

def get_diff_of_db_api_values(api_resp):
    db_resp = get_results()
    dicts_data = [obj['symbol'] for obj in db_resp]
    chunk_size = 100
    chunks = [dicts_data[i:i+chunk_size] for i in range(0, len(dicts_data), chunk_size)]
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        for chunk in chunks:
            executor.submit(task, db_resp, api_resp, chunk)

def task(db_resp, api_resp, data):
    for ele in data:
        db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
        if not db_match_data:
            continue
        api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
        if not api_match_data:
            continue
        
        api_last_price = float(api_match_data['lastPrice'])
        mar3_purchased = db_match_data['mar3_purchased']
        mar5_purchased = db_match_data['mar5_purchased']
        mar10_purchased = db_match_data['mar10_purchased']
        mar20_purchased = db_match_data['mar20_purchased']

        margin3count = int(db_match_data['margin3count'])
        margin5count = int(db_match_data['Margin5count'])
        margin10count = int(db_match_data['Margin10count'])
        margin20count = int(db_match_data['Margin20count'])

        mar3_checked = db_match_data['mar3']
        mar5_checked = db_match_data['mar5']
        mar10_checked = db_match_data['mar10']
        mar20_checked = db_match_data['mar20']

        if mar3_purchased < margin3count and not mar3_checked:
            db_margin = float(db_match_data['margin3'])
            margin_level = 'mar3'
        elif mar5_purchased < margin5count and not mar5_checked:
            db_margin = float(db_match_data['margin5'])
            margin_level = 'mar5'
        elif mar10_purchased < margin10count and not mar10_checked:
            db_margin = float(db_match_data['margin10'])
            margin_level = 'mar10'
        elif mar20_purchased < margin20count and not mar20_checked:
            db_margin = float(db_match_data['margin20'])
            margin_level = 'mar20'
        else:
            continue

        if api_last_price >= db_margin:
            print(db_margin)
            usd_amount = 5
            base_asset_symbol = ele.replace('USDT', '')
            print(base_asset_symbol)
            #buy_asset_with_usd(base_asset_symbol, usd_amount)
            update_margin_status(db_match_data['symbol'], margin_level)

def update_margin_status(symbol, margin_level):
    connection, cursor = get_db_connection()
    try:
        sql = f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s"
        cursor.execute(sql, (symbol,))
        connection.commit()
    except Exception as e:
        print(f"Error updating margin status: {e}")
    finally:
        cursor.close()
        connection.close()

def update_last_prices(api_resp):
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
    if not updates:
        return
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET lastPrice = %s WHERE symbol = %s"
        cursor.executemany(sql, updates)
        connection.commit()
    except Exception as e:
        print(f"Error updating last prices: {e}")
    finally:
        cursor.close()
        connection.close()

def get_active_trades():
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

#def show():
 #   while True:
  #      try:
   #         api_resp = get_data_from_wazirx()
    #        get_diff_of_db_api_values(api_resp)
     #       update_last_prices(api_resp)
      #      time.sleep(10)
       # except Exception as e:
        #    print(f"An error occurred: {e}")
         #   time.sleep(10)

#if __name__ == "__main__":
 #    show()
