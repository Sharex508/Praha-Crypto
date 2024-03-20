from tokenize import Double
import requests
from binance.spot import Spot as Client
#from binance.lib.utils import config_logging 
from datetime import datetime as dt
import json
import psycopg2
from psycopg2.extras import execute_values
import datetime
import time
import schedule
from notifications import notisend
from concurrent.futures import ThreadPoolExecutor
client = Client()
import pandas as pd
import ccxt





def get_db_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="prahacrypto08.cf0e8ug6ynu6.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="prahacrypto08")

    cursor = connection.cursor()
    return connection, cursor

def get_data_from_wazirx(filter='USDT'):
     data = requests.get('https://api.binance.com/api/v3/ticker/price').json()

    # Filter data to include only symbols that contain the currency in the filter.
     resp = [d for d in data if filter in d['symbol'] and 'price' in d]
    
    # Update each dictionary in resp with additional keys.
     for obj in resp:
        lprice = float(obj['price'])
        obj.update({
            "lastPrice" : lprice
            
        })
       # print('completed')
    
     return resp


def get_results():
    connection = None
    try:
        connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="prahacrypto08.cf0e8ug6ynu6.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="prahacrypto08")
        connection.autocommit = True

        cursor = connection.cursor()
        sql = "SELECT * FROM trading where status='0'"
        try:

            cursor.execute(sql)
            # Fetch all the rows in a list of lists.
            results = cursor.fetchall()
            keys = ('symbol', 'intialPrice', 'highPrice',
                   'lastPrice', 'margin')
            data = []
            for obj in results:
               data.append(dict(zip(keys, obj)))
             
            return data
            print(data)
        except Exception as e:
            print(e)
    except Exception as e:
            print(e)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    
def get_diff_of_db_api_values(api_resp):
    start = time.time()
    db_resp = get_results()
    #api_resp = get_data_from_wazirx()
    dicts_data = [obj['symbol'] for obj in db_resp]

    n = 1000
    chunks = [dicts_data[i:i+n] for i in range(0, len(dicts_data), n)]

    with ThreadPoolExecutor(max_workers=6) as executor:
        for chunk in chunks:
            executor.submit(task, db_resp, api_resp, chunk)
            #print(db_resp)

    done = time.time()
    elapsed = done - start
    #print(elapsed)



def task(db_resp, api_resp, data):
    for ele in data:
        db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
        if not db_match_data:
            continue
        api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
        if not api_match_data:
            continue
        if api_match_data['symbol'] == db_match_data['symbol']:
            api_last_price = float(api_match_data['lastPrice'])
            db_margin = float(db_match_data['margin'])
            initial_price = float(db_match_data['intialPrice'])

            if api_last_price >= db_margin:
                quantity = 1 / api_last_price
                dbdata = {"symbol": ele, "side": "buy", "type": "limit", "price": api_last_price, "quantity": quantity, "recvWindow": 10000, "timestamp": int(time.time() * 1000)}
                notisend({"symbol": ele, "side": "buy", "type": "limit", "initial_price": initial_price, "purchasing_price": api_last_price, "db_margin": db_margin, "quantity": quantity})
                #create_limit_buy_order(dbdata['symbol'].replace('USDT', '/USDT'), dbdata['quantity'], dbdata['price'], dbdata)

                # Update coin record here
                update_coin_record(dbdata)



def update_coin_record(dbdata):
    try:
        print("came to database update")
        connection, cursor = get_db_connection()
        sql = "UPDATE trading SET status = %s, purchasePrice = %s, quantity = %s WHERE symbol = %s"
        cursor.execute(sql, (1, dbdata['price'], dbdata['quantity'], dbdata['symbol']))
        connection.commit()
        print("Record updated successfully")
    except Exception as e:
        print("Error while updating record:", e)
    finally:
        cursor.close()
        connection.close()



def get_active_trades():
    """Retrieve active trades with status = 1."""
    connection, cursor = get_db_connection()
    try:
        sql = "SELECT * FROM trading WHERE status='1'"
        cursor.execute(sql)
        results = cursor.fetchall()
        keys = ('symbol', 'initialPrice', 'highPrice', 'lastPrice', 'margin', 'purchasePrice', 'quantity', 'created_at', 'status')
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        print(f"Error fetching active trades: {e}")
    finally:
        cursor.close()
        connection.close()

def update_last_prices(api_resp):
    """Update last prices for active trades using existing API response."""
    db_resp = get_active_trades()
    for trade in db_resp:
        symbol = trade['symbol']
        matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
        if matching_api_data:
            new_last_price = matching_api_data['lastPrice']
            update_coin_last_price(symbol, new_last_price)


def update_coin_last_price(symbol, last_price):
    """Update the last price of a coin in the trading table."""
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET lastPrice = %s WHERE symbol = %s"
        cursor.execute(sql, (last_price, symbol))
        connection.commit()
        print(f"Updated last price for {symbol} to {last_price}")
    except Exception as e:
        print(f"Error updating last price for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()


def show():
    while True:
        try:
            api_resp = get_data_from_wazirx()  # Fetch current prices once
            get_diff_of_db_api_values(api_resp)  # Use the data for your existing logic
            update_last_prices(api_resp) 
            time.sleep(10)
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(10)

show()

