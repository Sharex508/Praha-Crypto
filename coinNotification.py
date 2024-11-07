import psycopg2
import requests
from notifications import notisend
from decimal import Decimal, getcontext
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)

# Set decimal precision
getcontext().prec = 6  # Adjust precision as needed

def get_db_connection():
    connection = psycopg2.connect(
        user="postgres",
        password="Harsha508",
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="HarshaCry"
    )
    return connection, connection.cursor()

def get_active_trades():
    connection, cursor = get_db_connection()
    try:
        sql = "SELECT * FROM trading WHERE status = '1'"
        cursor.execute(sql)
        results = cursor.fetchall()
        keys = (
            'symbol', 'intialprice', 'highprice', 'lastprice', 'margin3', 'margin5',
            'margin10', 'margin20', 'purchaseprice', 'mar3', 'mar5', 'mar10', 'mar20',
            'created_at', 'status', 'last_notified_decrease_percentage', 'last_notified_percentage'
        )
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        logging.error(f"Error fetching active trades: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def get_data_from_wazirx(filter='USDT'):
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    return [d for d in data if filter in d['symbol'] and 'price' in d]

def update_high_price(symbol, new_high_price):
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET highprice = %s, last_notified_decrease_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (str(new_high_price), 0.0, symbol))
        connection.commit()
    except Exception as e:
        logging.error(f"Error updating highprice for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_notified_percentage(symbol, percentage):
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET last_notified_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (percentage, symbol))
        connection.commit()
    except Exception as e:
        logging.error(f"Error updating last_notified_percentage for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_notified_decrease_percentage(symbol, percentage):
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET last_notified_decrease_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (percentage, symbol))
        connection.commit()
    except Exception as e:
        logging.error(f"Error updating last_notified_decrease_percentage for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def notify_price_changes(api_resp):
    db_resp = get_active_trades()
    increased_coins = []
    decreased_coins = []

    for trade in db_resp:
        try:
            symbol = trade['symbol']
            initial_price = Decimal(trade['intialprice'])
            last_notified = Decimal(trade.get('last_notified_percentage') or '0.0')
            last_notified_decrease = Decimal(trade.get('last_notified_decrease_percentage') or '0.0')
            high_price = Decimal(trade['highprice'])

            matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
            if matching_api_data:
                current_price = Decimal(matching_api_data['price'])
                percentage_increase = ((current_price - initial_price) / initial_price) * Decimal('100')
                percentage_decrease_from_high = ((high_price - current_price) / high_price) * Decimal('100')

                # For increase notifications at every 5% increment
                next_increment = ((last_notified // Decimal('5')) + 1) * Decimal('5')
                if percentage_increase >= next_increment:
                    increased_coins.append({
                        "symbol": symbol,
                        "purchase_price": initial_price,
                        "percentage_change": float(percentage_increase)
                    })
                    update_notified_percentage(symbol, float(next_increment))
                    last_notified = next_increment

                # For decrease notifications at every 5% decrement
                next_decrement = ((last_notified_decrease // Decimal('5')) + 1) * Decimal('5')
                if (percentage_decrease_from_high >= next_decrement) and (percentage_decrease_from_high >= Decimal('5')):
                    decreased_coins.append({
                        "symbol": symbol,
                        "purchase_price": initial_price,
                        "percentage_change": float(percentage_decrease_from_high)
                    })
                    update_notified_decrease_percentage(symbol, float(next_decrement))
                    last_notified_decrease = next_decrement

            else:
                logging.warning(f"No matching API data for {symbol}")
        except TypeError as e:
            logging.error(f"TypeError encountered for {trade['symbol']}: {e}. Skipping.")
        except Exception as e:
            logging.error(f"Error processing {trade['symbol']}: {e}")

    # Sending notification with categorized format
    notification_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Time: {notification_time}\n\nIncreased Coins:\n"
    message += "\n".join(
        [f"{coin['symbol']}, Purchase Price: {coin['purchase_price']}, Increase: {coin['percentage_change']:.2f}%"
         for coin in increased_coins]
    ) or "No coins increased."

    message += "\n\nDecreased Coins:\n"
    message += "\n".join(
        [f"{coin['symbol']}, Purchase Price: {coin['purchase_price']}, Decrease: {coin['percentage_change']:.2f}%"
         for coin in decreased_coins]
    ) or "No coins decreased."

    notisend(message)
    logging.info(f"Notification sent: {message}")

if __name__ == "__main__":
    api_resp = get_data_from_wazirx()
    notify_price_changes(api_resp)
