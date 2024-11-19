# buy.py

import time
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
from binance.spot import Spot as Client
from buynoti import notisend  # Ensure this module exists and is properly configured
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define take-profit percentage
TAKE_PROFIT_PERCENTAGE = 10  # 10% increase

# Set up logging to log both to file and console with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("buy.log"),
        logging.StreamHandler()
    ]
)

# Initialize a lock for synchronizing access to the purchase counts
purchase_lock = threading.Lock()

# Initialize Binance client with API keys from environment variables
API_KEY = os.getenv('BINANCE_API_KEY')       # Ensure this environment variable is set
API_SECRET = os.getenv('BINANCE_API_SECRET') # Ensure this environment variable is set
client = Client(API_KEY, API_SECRET)

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME')
        )
        logging.info("Successfully connected to the PostgreSQL database.")
        return connection, connection.cursor()
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")
        raise

def get_data_from_binance(filter='USDT'):
    """Fetch price data from Binance API."""
    try:
        response = requests.get('https://api.binance.com/api/v3/ticker/price', timeout=10)
        response.raise_for_status()
        data = response.json()
        resp = [d for d in data if filter in d['symbol'] and 'price' in d]
        logging.info("Successfully fetched data from Binance API.")
        return resp
    except requests.RequestException as e:
        logging.error(f"Error fetching data from Binance API: {e}")
        return []

def get_coin_limits():
    """Fetch coin limits from the Coinnumber table."""
    connection, cursor = get_db_connection()
    try:
        # Get Coinnumber limits with LIMIT 1 assuming single row
        sql_limits = "SELECT margin3count, margin5count, margin10count, margin20count, amount FROM Coinnumber LIMIT 1"
        cursor.execute(sql_limits)
        limits = cursor.fetchone()

        if not limits:
            logging.error("No rows found in Coinnumber table.")
            return None

        # Ensure all required columns are present
        if len(limits) < 5:
            logging.error("Incomplete data fetched from Coinnumber table.")
            return None

        coin_limits = {
            "margin3count": int(float(limits[0] or 0)),
            "margin5count": int(float(limits[1] or 0)),
            "margin10count": int(float(limits[2] or 0)),
            "margin20count": int(float(limits[3] or 0)),
            "amount": float(limits[4] or 0.0)
        }

        logging.info(f"Coin limits retrieved: {coin_limits}")
        return coin_limits
    except Exception as e:
        logging.error(f"Error fetching coin limits: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def get_trading_sums():
    """Fetch the number of purchased coins at each margin level."""
    connection, cursor = get_db_connection()
    try:
        # Get sum of mar3, mar5, mar10, mar20 from trading table where status = '1' (purchased coins)
        sql_sum = """
        SELECT SUM(CASE WHEN mar3 THEN 1 ELSE 0 END) AS sum_mar3,
               SUM(CASE WHEN mar5 THEN 1 ELSE 0 END) AS sum_mar5,
               SUM(CASE WHEN mar10 THEN 1 ELSE 0 END) AS sum_mar10,
               SUM(CASE WHEN mar20 THEN 1 ELSE 0 END) AS sum_mar20
        FROM trading
        WHERE status = '1'
        """
        cursor.execute(sql_sum)
        trading_sums = cursor.fetchone()

        trading_summary = {
            "sum_mar3": int(trading_sums[0] or 0),
            "sum_mar5": int(trading_sums[1] or 0),
            "sum_mar10": int(trading_sums[2] or 0),
            "sum_mar20": int(trading_sums[3] or 0)
        }

        logging.info(f"Trading summary: {trading_summary}")
        return trading_summary
    except Exception as e:
        logging.error(f"Error fetching trading sums: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def get_results():
    """Fetch non-purchased trading records from the database."""
    connection, cursor = get_db_connection()
    try:
        sql = """
        SELECT symbol, initialPrice, highPrice, lastPrice, margin3, margin5, margin10, margin20, purchasePrice, stopLossPrice,
               mar3, mar5, mar10, mar20
        FROM trading
        WHERE status != '1'
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        keys = ('symbol', 'initialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 'margin10', 'margin20',
                'purchasePrice', 'stopLossPrice', 'mar3', 'mar5', 'mar10', 'mar20')

        data = [dict(zip(keys, obj)) for obj in results]

        logging.info(f"Fetched {len(data)} trading records from the database.")
        return data
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def buy_asset_with_usd(asset_symbol, usd_amount):
    """
    Purchase an amount of asset specified in USD using Binance Spot API and calculate take-profit price.
    
    Parameters:
    asset_symbol (str): The symbol of the asset to buy (e.g., 'NEIROUSDT').
    usd_amount (float): The USD amount to spend on the purchase, assuming USD ≈ USDT.
    
    Returns:
    tuple: (average_price, take_profit_price) if successful, else (None, None)
    """
    # Convert USD amount to USDT amount, assuming 1 USD ≈ 1 USDT
    usdt_amount = usd_amount  # This example assumes 1 USD ≈ 1 USDT
    
    # Specify the market pair (e.g., NEIROUSDT)
    asset_pair = asset_symbol 
    
    try:
        # Create a market buy order by specifying the amount of USDT to spend
        order = client.new_order(
            symbol=asset_pair,
            side='BUY',
            type='MARKET',
            quoteOrderQty=str(usdt_amount)  # Convert to string as required by the API
        )
        
        # Extract the executed price from the order response
        fills = order.get('fills', [])
        if not fills:
            raise ValueError("No fills found in the order response.")
        
        total_cost = 0.0
        total_qty = 0.0
        for fill in fills:
            price = float(fill.get('price', 0.0))
            qty = float(fill.get('qty', 0.0))
            total_cost += price * qty
            total_qty += qty
        
        if total_qty == 0:
            raise ValueError("Total quantity purchased is zero.")
        
        average_price = total_cost / total_qty
        
        # Calculate take-profit price (10% increase from the purchase price)
        take_profit_price = average_price * (1 + TAKE_PROFIT_PERCENTAGE / 100)
        
        success_message = f"Successfully purchased {asset_symbol} worth ${usd_amount} USD at an average price of {average_price:.2f} USDT. Take-profit set at {take_profit_price:.2f} USDT."
        print(success_message)
        notisend(success_message)  # Send notification
        logging.info(success_message)
        
        return average_price, take_profit_price  # Return the executed purchase price and take-profit price
        
    except Exception as e:
        error_message = f"An error occurred while purchasing {asset_symbol}: {e}"
        print(error_message)
        notisend(error_message)  # Send error notification
        logging.error(error_message)
        return None, None  # Indicate failure

def place_take_profit_order(asset_symbol, take_profit_price, quantity):
    """
    Place a take-profit order for the purchased asset.
    
    Parameters:
    asset_symbol (str): The symbol of the asset to set a take-profit for (e.g., 'NEIROUSDT').
    take_profit_price (float): The price at which to trigger the take-profit.
    quantity (float): The quantity of the asset to sell.
    
    Returns:
    str: The order ID of the placed take-profit order if successful, else None
    """
    try:
        # Place a TAKE_PROFIT_LIMIT order
        order = client.new_order(
            symbol=asset_symbol,
            side='SELL',
            type='TAKE_PROFIT_LIMIT',
            timeInForce='GTC',
            quantity=quantity,
            price=str(round(take_profit_price * 0.99, 2)),  # Set the limit price slightly below take-profit price
            stopPrice=str(round(take_profit_price, 2))
        )
        
        order_id = order.get('orderId')
        if not order_id:
            raise ValueError("Failed to retrieve order ID from take-profit order response.")
        
        success_message = f"Take-profit order placed for {asset_symbol} at {take_profit_price:.2f} USDT (Order ID: {order_id})."
        print(success_message)
        notisend(success_message)  # Send notification
        logging.info(success_message)
        
        return str(order_id)
    except Exception as e:
        error_message = f"An error occurred while placing take-profit order for {asset_symbol}: {e}"
        print(error_message)
        notisend(error_message)  # Send error notification
        logging.error(error_message)
        return None

def update_margin_status(symbol, margin_level, purchase_price, take_profit_order_id, take_profit_price):
    """
    Update the margin status, purchase price, and take-profit details in the database for a purchased coin.
    
    Parameters:
    symbol (str): The symbol of the asset purchased (e.g., 'NEIROUSDT').
    margin_level (str): The margin level at which the asset was purchased (e.g., 'mar3').
    purchase_price (float): The executed purchase price of the asset.
    take_profit_order_id (str): The order ID of the placed take-profit order.
    take_profit_price (float): The take-profit price.
    """
    connection, cursor = get_db_connection()
    try:
        # Prepare the SQL statement to update the margin level, status, purchasePrice, takeProfitOrderId, and takeProfitPrice
        sql_update_trading = f"""
            UPDATE trading 
            SET {margin_level} = TRUE, 
                status = '1', 
                purchasePrice = %s,
                takeProfitOrderId = %s,
                takeProfitPrice = %s
            WHERE symbol = %s
        """
        cursor.execute(sql_update_trading, (purchase_price, take_profit_order_id, take_profit_price, symbol))
        connection.commit()
        logging.info(f"{symbol} purchased at {margin_level} with price {purchase_price}. Take-profit set at {take_profit_price}. Status updated to 1.")
    except Exception as e:
        logging.error(f"Error updating margin status for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def task(db_resp, api_resp, coin_limits, trading_summary, data):
    """Process each chunk of data and make purchases based on the margin logic."""
    try:
        for ele in data:
            db_match_data = next((item for item in db_resp if item["symbol"] == ele["symbol"]), None)
            if not db_match_data:
                continue
            api_match_data = next((item for item in api_resp if item["symbol"] == ele["symbol"]), None)
            if not api_match_data:
                continue

            api_last_price = float(api_match_data['price'] or 0.0)
            db_price = float(db_match_data.get("initialPrice") or 0.0)

            # Convert the margin values from string to float before comparison
            margin3 = float(db_match_data["margin3"] or 0.0)
            margin5 = float(db_match_data["margin5"] or 0.0)
            margin10 = float(db_match_data["margin10"] or 0.0)
            margin20 = float(db_match_data["margin20"] or 0.0)

            # Use the lock to ensure that updates to the trading_summary are thread-safe
            with purchase_lock:
                # Check and purchase at margin3 if limit not reached
                if trading_summary["sum_mar3"] < coin_limits["margin3count"]:
                    if api_last_price >= margin3 and not db_match_data['mar3']:
                        logging.info(f"Purchasing {ele['symbol']} at 3% margin.")
                        purchase_price, take_profit_price = buy_asset_with_usd(ele['symbol'], coin_limits["amount"])
                        if purchase_price:
                            # Determine the quantity purchased
                            quantity = coin_limits["amount"] / purchase_price
                            take_profit_order_id = place_take_profit_order(ele['symbol'], take_profit_price, quantity)
                            if take_profit_order_id:
                                update_margin_status(db_match_data['symbol'], 'mar3', purchase_price, take_profit_order_id, take_profit_price)
                                trading_summary["sum_mar3"] += 1
                    continue

                # Check and purchase at margin5 if limit not reached
                if trading_summary["sum_mar5"] < coin_limits["margin5count"]:
                    if api_last_price >= margin5 and not db_match_data['mar5']:
                        logging.info(f"Purchasing {ele['symbol']} at 5% margin.")
                        purchase_price, take_profit_price = buy_asset_with_usd(ele['symbol'], coin_limits["amount"])
                        if purchase_price:
                            quantity = coin_limits["amount"] / purchase_price
                            take_profit_order_id = place_take_profit_order(ele['symbol'], take_profit_price, quantity)
                            if take_profit_order_id:
                                update_margin_status(db_match_data['symbol'], 'mar5', purchase_price, take_profit_order_id, take_profit_price)
                                trading_summary["sum_mar5"] += 1
                    continue

                # Check and purchase at margin10 if limit not reached
                if trading_summary["sum_mar10"] < coin_limits["margin10count"]:
                    if api_last_price >= margin10 and not db_match_data['mar10']:
                        logging.info(f"Purchasing {ele['symbol']} at 10% margin.")
                        purchase_price, take_profit_price = buy_asset_with_usd(ele['symbol'], coin_limits["amount"])
                        if purchase_price:
                            quantity = coin_limits["amount"] / purchase_price
                            take_profit_order_id = place_take_profit_order(ele['symbol'], take_profit_price, quantity)
                            if take_profit_order_id:
                                update_margin_status(db_match_data['symbol'], 'mar10', purchase_price, take_profit_order_id, take_profit_price)
                                trading_summary["sum_mar10"] += 1
                    continue

                # Check and purchase at margin20 if limit not reached
                if trading_summary["sum_mar20"] < coin_limits["margin20count"]:
                    if api_last_price >= margin20 and not db_match_data['mar20']:
                        logging.info(f"Purchasing {ele['symbol']} at 20% margin.")
                        purchase_price, take_profit_price = buy_asset_with_usd(ele['symbol'], coin_limits["amount"])
                        if purchase_price:
                            quantity = coin_limits["amount"] / purchase_price
                            take_profit_order_id = place_take_profit_order(ele['symbol'], take_profit_price, quantity)
                            if take_profit_order_id:
                                update_margin_status(db_match_data['symbol'], 'mar20', purchase_price, take_profit_order_id, take_profit_price)
                                trading_summary["sum_mar20"] += 1
                    continue

    except Exception as e:
        logging.error(f"Error in task processing: {e}")

def show():
    """Execute the buying logic once and return."""
    try:
        logging.info(f"Executing buying logic at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Get API data
        api_resp = get_data_from_binance()
        if not api_resp:
            logging.error("Failed to retrieve data from Binance API.")
            return

        # Get coin limits and trading sums
        coin_limits = get_coin_limits()
        trading_summary = get_trading_sums()
        if not coin_limits or not trading_summary:
            logging.error("Error: Failed to retrieve necessary data from the database.")
            return

        # Get DB data
        db_resp = get_results()
        if not db_resp:
            logging.error("No trading records found.")
            return

        # Prepare symbols for processing
        dicts_data = db_resp  # Each item is a dict with symbol and other details
        chunk_size = min(20, len(dicts_data))
        chunks = [dicts_data[i:i + chunk_size] for i in range(0, len(dicts_data), chunk_size)]

        with ThreadPoolExecutor(max_workers=4) as executor:
            for chunk in chunks:
                executor.submit(task, db_resp, api_resp, coin_limits, trading_summary, chunk)

    except Exception as e:
        logging.error(f"An error occurred in show(): {e}")

if __name__ == "__main__":
    show()
