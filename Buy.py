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
    logging.debug(f"DEBUG - Fetched {len(resp)} symbols from API with filter '{filter}'.")
    return resp

def get_coin_limits_and_trading_sums():
    connection, cursor = get_db_connection()
    try:
        # Get Coinnumber limits
        sql_limits = "SELECT margin3count, margin5count, margin10count, margin20count, amount FROM Coinnumber"
        cursor.execute(sql_limits)
        limits = cursor.fetchone()

        coin_limits = {
            "margin3count": int(float(limits[0] or 0)),
            "margin5count": int(float(limits[1] or 0)),
            "margin10count": int(float(limits[2] or 0)),
            "margin20count": int(float(limits[3] or 0)),
            "amount": float(limits[4] or 0.0)
        }

        # Get sum of mar3, mar5, mar10, mar20 from trading table where status != '1' (non-purchased coins)
        sql_sum = """
        SELECT SUM(CASE WHEN mar3 THEN 1 ELSE 0 END) AS sum_mar3,
               SUM(CASE WHEN mar5 THEN 1 ELSE 0 END) AS sum_mar5,
               SUM(CASE WHEN mar10 THEN 1 ELSE 0 END) AS sum_mar10,
               SUM(CASE WHEN mar20 THEN 1 ELSE 0 END) AS sum_mar20
        FROM trading
        WHERE status != '1'
        """
        cursor.execute(sql_sum)
        trading_sums = cursor.fetchone()

        trading_summary = {
            "sum_mar3": int(trading_sums[0] or 0),
            "sum_mar5": int(trading_sums[1] or 0),
            "sum_mar10": int(trading_sums[2] or 0),
            "sum_mar20": int(trading_sums[3] or 0)
        }

        logging.debug(f"DEBUG - Coin Limits: {coin_limits}, Trading Sums (non-purchased): {trading_summary}")
        return coin_limits, trading_summary
    except Exception as e:
        logging.error(f"Error fetching coin limits and trading sums: {e}")
        return None, None
    finally:
        cursor.close()
        connection.close()

def get_results():
    connection, cursor = get_db_connection()
    try:
        # Fetch full set of fields needed for processing, but only for rows where status != '1'
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice,
               mar3, mar5, mar10, mar20
        FROM trading
        WHERE status != '1'  -- Compare status as a string, because status is of type TEXT
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        # Define the keys corresponding to the fields in the result
        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20', 
                'purchasePrice', 'mar3', 'mar5', 'mar10', 'mar20')

        # Zip the results with the keys and convert them into a list of dictionaries
        data = [dict(zip(keys, obj)) for obj in results]

        logging.debug(f"DEBUG - Fetched {len(data)} non-purchased trading records from the database.")
        return data
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
    finally:
        cursor.close()
        connection.close()

def task(db_resp, api_resp, coin_limits, trading_summary, data):
    try:
        for ele in data:
            db_match_data = next((item for item in db_resp if item["symbol"] == ele), None)
            if not db_match_data:
                logging.debug(f"DEBUG - Symbol {ele} not found in DB data.")
                continue
            api_match_data = next((item for item in api_resp if item["symbol"] == ele), None)
            if not api_match_data:
                logging.debug(f"DEBUG - Symbol {ele} not found in API data.")
                continue

            api_last_price = float(api_match_data['price'] or 0.0)
            db_price = float(db_match_data.get("intialPrice") or 0.0)

            logging.debug(f"\nProcessing {ele}\nLast Price: {api_last_price}\nDB Price: {db_price}\nCoin Limits: {coin_limits}")

            # Convert the margin values from string to float before comparison
            margin3 = float(db_match_data["margin3"] or 0.0)
            margin5 = float(db_match_data["margin5"] or 0.0)
            margin10 = float(db_match_data["margin10"] or 0.0)
            margin20 = float(db_match_data["margin20"] or 0.0)

            # Check and purchase at margin3 if limit not reached
            if trading_summary["sum_mar3"] < coin_limits["margin3count"]:
                if api_last_price >= margin3:
                    logging.debug(f"DEBUG - Purchasing {ele} at 3% margin. Current count: {trading_summary['sum_mar3']} out of {coin_limits['margin3count']}")
                    update_margin_status(db_match_data['symbol'], 'mar3')
                    trading_summary["sum_mar3"] += 1
                else:
                    logging.debug(f"DEBUG - {ele} did not meet the margin3 condition. Last price: {api_last_price}, Required: {margin3}")
                continue

            # Check and purchase at margin5 if limit not reached
            if trading_summary["sum_mar5"] < coin_limits["margin5count"]:
                if api_last_price >= margin5:
                    logging.debug(f"DEBUG - Purchasing {ele} at 5% margin. Current count: {trading_summary['sum_mar5']} out of {coin_limits['margin5count']}")
                    update_margin_status(db_match_data['symbol'], 'mar5')
                    trading_summary["sum_mar5"] += 1
                else:
                    logging.debug(f"DEBUG - {ele} did not meet the margin5 condition. Last price: {api_last_price}, Required: {margin5}")
                continue

            # Check and purchase at margin10 if limit not reached
            if trading_summary["sum_mar10"] < coin_limits["margin10count"]:
                if api_last_price >= margin10:
                    logging.debug(f"DEBUG - Purchasing {ele} at 10% margin. Current count: {trading_summary['sum_mar10']} out of {coin_limits['margin10count']}")
                    update_margin_status(db_match_data['symbol'], 'mar10')
                    trading_summary["sum_mar10"] += 1
                else:
                    logging.debug(f"DEBUG - {ele} did not meet the margin10 condition. Last price: {api_last_price}, Required: {margin10}")
                continue

            # Check and purchase at margin20 without limit
            if api_last_price >= margin20:
                logging.debug(f"DEBUG - Purchasing {ele} at 20% margin. No limit on purchases.")
                update_margin_status(db_match_data['symbol'], 'mar20')
                trading_summary["sum_mar20"] += 1
            else:
                logging.debug(f"DEBUG - {ele} did not meet the margin20 condition. Last price: {api_last_price}, Required: {margin20}")
    
    except Exception as e:
        logging.error(f"Error in task processing {ele}: {e}")

def update_margin_status(symbol, margin_level):
    connection, cursor = get_db_connection()
    try:
        # Update the trading table for the purchased symbol
        sql_update_trading = f"UPDATE trading SET {margin_level} = TRUE, status = '1' WHERE symbol = %s"
        cursor.execute(sql_update_trading, (symbol,))
        connection.commit()
        logging.info(f"{symbol} purchased at {margin_level}. Status updated to 1.")
    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def get_diff_of_db_api_values(api_resp):
    # Get DB results and compute the necessary limits and sums
    db_resp = get_results()
    coin_limits, trading_summary = get_coin_limits_and_trading_sums()
    
    if not db_resp or not coin_limits or not trading_summary:
        logging.error("Error: Failed to retrieve necessary data from the database.")
        return None, None, None

    logging.debug("DEBUG - Completed data retrieval and pre-calculations.")
    return db_resp, coin_limits, trading_summary

def show():
    while True:
        try:
            logging.info(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Get API data
            api_resp = get_data_from_wazirx()
            if not api_resp:
                logging.error("Failed to retrieve data from WazirX API.")
                time.sleep(60)
                continue

            # Get DB and pre-calculated values
            db_resp, coin_limits, trading_summary = get_diff_of_db_api_values(api_resp)
            if not db_resp:
                time.sleep(60)
                continue

            # Prepare symbols for processing
            dicts_data = [obj['symbol'] for obj in db_resp]
            chunk_size = min(20, len(dicts_data))
            chunks = [dicts_data[i:i + chunk_size] for i in range(0, len(dicts_data), chunk_size)]

            logging.debug(f"DEBUG - Total Chunks Created: {len(chunks)}. Total Records: {len(dicts_data)}")

            with ThreadPoolExecutor(max_workers=4) as executor:
                for idx, chunk in enumerate(chunks):
                    logging.debug(f"DEBUG - Submitting chunk {idx + 1}/{len(chunks)} with {len(chunk)} records")
                    executor.submit(task, db_resp, api_resp, coin_limits, trading_summary, chunk)

            time.sleep(60)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(60)

if __name__ == "__main__":
    show()
