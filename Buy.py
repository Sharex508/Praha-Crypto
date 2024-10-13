import time
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor
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
    resp = [d for d in data if filter in d['symbol'] and 'price' in d]
    logging.debug(f"Fetched {len(resp)} symbols from API with filter '{filter}'.")
    return resp

def get_results():
    connection, cursor = get_db_connection()
    try:
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
               mar3, mar5, mar10, mar20, status
        FROM trading
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'mar3', 'mar5', 'mar10', 'mar20', 'status')
        data = [dict(zip(keys, obj)) for obj in results]
        logging.debug(f"Fetched {len(data)} trading records from the database.")
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
            "margin3count": int(float(limits[0] or 0)),
            "margin5count": int(float(limits[1] or 0)),
            "margin10count": int(float(limits[2] or 0)),
            "margin20count": int(float(limits[3] or 0)),
            "amount": float(limits[4] or 0.0)
        }
        logging.debug(f"Coin Limits: {coin_limits}")
        return coin_limits
    except Exception as e:
        logging.error(f"Error fetching coin limits: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def get_diff_of_db_api_values(api_resp):
    db_resp = get_results()
    symbols_to_process = [obj['symbol'] for obj in db_resp]
    chunk_size = min(20, len(symbols_to_process))
    chunks = [symbols_to_process[i:i + chunk_size] for i in range(0, len(symbols_to_process), chunk_size)]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        for chunk in chunks:
            executor.submit(task, db_resp, api_resp, chunk)

def task(db_resp, api_resp, data):
    coin_limits = get_coin_limits()
    if not coin_limits:
        logging.error("Error: Coin limits not found.")
        return

    for ele in data:
        db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
        if not db_match_data or db_match_data['status'] == '1':
            logging.debug(f"Skipping {ele} as it is already purchased or not found in DB.")
            continue
        api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
        if not api_match_data:
            logging.debug(f"Skipping {ele} as it is not found in API data.")
            continue

        api_last_price = float(api_match_data['price'] or 0.0)
        db_price = float(db_match_data.get("intialPrice") or 0.0)
        
        db_margin, margin_level, matched_percentage = calculate_margin_level(db_match_data, coin_limits, api_last_price)

        if db_margin and api_last_price >= db_margin:
            amount = coin_limits['amount']
            base_asset_symbol = ele.replace('USDT', '')

            logging.info(
                f"Action: Buying {base_asset_symbol}\n"
                f"Level: {margin_level}\n"
                f"Required Margin: {db_margin}\n"
                f"Amount: {amount}\n"
                f"Matched at Percentage: {matched_percentage}%"
            )

            status_updated = update_margin_status(db_match_data['symbol'], margin_level)
            if status_updated:
                logging.debug(f"{ele} processed at {matched_percentage}% margin level {margin_level}. Status updated.")
            else:
                logging.debug(f"{ele} processed at {matched_percentage}% margin level {margin_level}. Status update failed.")
        else:
            logging.debug(
                f"No action for {ele}.\n"
                f"Current Price: {api_last_price}\n"
                f"Margin Level: {margin_level or 'N/A'}"
            )

def calculate_margin_level(db_match_data, coin_limits, last_price):
    margin_levels = [
        ("mar3", "margin3", coin_limits['margin3count'], 3),
        ("mar5", "margin5", coin_limits['margin5count'], 5),
        ("mar10", "margin10", coin_limits['margin10count'], 10),
        ("mar20", "margin20", coin_limits['margin20count'], 20)
    ]

    for level_flag, margin_field, limit, percentage in margin_levels:
        if not db_match_data[level_flag] and float(db_match_data[margin_field] or 0.0) <= last_price:
            return float(db_match_data[margin_field] or 0.0), level_flag, percentage
    
    return None, None, None

def update_margin_status(symbol, margin_level):
    connection, cursor = get_db_connection()
    status_updated = False
    try:
        sql_update_trading = f"""
            UPDATE trading
            SET {margin_level} = TRUE, status = '1'
            WHERE symbol = %s AND status != '1'
        """
        cursor.execute(sql_update_trading, (symbol,))

        margin_column = f"{margin_level}count"
        sql_update_coinnumber = f"UPDATE Coinnumber SET {margin_column} = {margin_column}::integer - 1"
        cursor.execute(sql_update_coinnumber)

        connection.commit()
        status_updated = cursor.rowcount > 0

        if status_updated:
            logging.info(f"{symbol} purchased at {margin_level}. Remaining count updated.")
        else:
            logging.info(f"{symbol} was already processed at {margin_level} and not updated again.")

    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()
    
    return status_updated

def update_last_prices(api_resp):
    db_resp = get_active_trades()
    updates = []
    for trade in db_resp:
        symbol = trade['symbol']
        matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
        if matching_api_data:
            new_last_price = matching_api_data['price']
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
        logging.info(f"Updated last prices for {len(updates)} symbols")
    except Exception as e:
        logging.error(f"Error updating last prices: {e}")
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
        logging.error(f"Error fetching active trades: {e}")
    finally:
        cursor.close()
        connection.close()

def show():
    while True:
        try:
            logging.info(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            api_resp = get_data_from_wazirx()
            get_diff_of_db_api_values(api_resp)
            time.sleep(10)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(60)

if __name__ == "__main__":
    show()
