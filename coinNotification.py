import psycopg2
import requests
from notifications import notisend
import math
     
def get_db_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Harsha508",
                                  host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
                                  port="5432",
                                  database="HarshaCry")
    return connection, connection.cursor()

def get_active_trades():
    connection, cursor = get_db_connection()
    try:
        sql = "SELECT * FROM trading WHERE status = '1'"
        cursor.execute(sql)
        results = cursor.fetchall()
        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 
                'margin10', 'margin20', 'purchasePrice', 'quantity', 'created_at', 'status',
                'last_notified_percentage', 'last_notified_decrease_percentage')
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        print(f"Error fetching active trades: {e}")
    finally:
        cursor.close()
        connection.close()

def get_data_from_wazirx(filter='USDT'):
    data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
    return [d for d in data if filter in d['symbol'] and 'price' in d]

def send_notification(symbol, initial_price, current_price, direction, percentage_change):
    notisend({
        "symbol": symbol,
        "side": direction,
        "type": "limit",
        "initial_price": initial_price,
        "purchasing_price": current_price,
        "percentage_change": percentage_change,
        "quantity": 1 / current_price
    })
    print(f"Notification sent for {symbol}: {direction} {percentage_change}%")

def update_high_price(symbol, new_high_price):
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET highPrice = %s, last_notified_decrease_percentage = 0 WHERE symbol = %s"
        cursor.execute(sql, (str(new_high_price), symbol))
        connection.commit()
    except Exception as e:
        print(f"Error updating highPrice for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_notified_percentage(symbol, percentage):
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET last_notified_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (str(percentage), symbol))
        connection.commit()
    except Exception as e:
        print(f"Error updating last_notified_percentage for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_notified_decrease_percentage(symbol, percentage):
    connection, cursor = get_db_connection()
    try:
        sql = "UPDATE trading SET last_notified_decrease_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (str(percentage), symbol))
        connection.commit()
    except Exception as e:
        print(f"Error updating last_notified_decrease_percentage for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def notify_price_increase(api_resp):
    db_resp = get_active_trades()

    for trade in db_resp:
        try:
            symbol = trade['symbol']
            initial_price = float(trade['intialPrice'])
            last_notified = float(trade.get('last_notified_percentage', 0.0) or 0.0)
            last_notified_decrease = float(trade.get('last_notified_decrease_percentage', 0.0) or 0.0)
            high_price = float(trade['highPrice'])

            matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
            if matching_api_data:
                current_price = float(matching_api_data['price'])
                percentage_increase = ((current_price - initial_price) / initial_price) * 100
                percentage_increase = round(percentage_increase, 2)

                # Update high price if current price exceeds it
                if current_price > high_price:
                    update_high_price(symbol, current_price)
                    high_price = current_price  # Update in variable too
                    last_notified_decrease = 0  # Reset decrease notifications

                # For increase notifications at every 5% increment
                next_notified_percentage = last_notified + 5
                if percentage_increase >= next_notified_percentage:
                    # Calculate the nearest multiple of 5 less than or equal to percentage_increase
                    new_last_notified = 5 * (percentage_increase // 5)
                    send_notification(symbol, initial_price, current_price, "increase", percentage_increase)
                    update_notified_percentage(symbol, new_last_notified)

                # Calculate percentage decrease from high price
                percentage_decrease_from_high = ((high_price - current_price) / high_price) * 100
                percentage_decrease_from_high = round(percentage_decrease_from_high, 2)

                # For decrease notifications at every 5% decrement
                next_notified_decrease_percentage = last_notified_decrease + 5
                if percentage_decrease_from_high >= next_notified_decrease_percentage and percentage_decrease_from_high >= 5:
                    # Calculate the nearest multiple of 5 less than or equal to percentage_decrease_from_high
                    new_last_notified_decrease = 5 * (percentage_decrease_from_high // 5)
                    send_notification(symbol, initial_price, current_price, "decrease", percentage_decrease_from_high)
                    update_notified_decrease_percentage(symbol, new_last_notified_decrease)

            else:
                print(f"No matching API data for {symbol}")
        except TypeError as e:
            print(f"TypeError encountered for {trade['symbol']}: {e}. Skipping.")
        except Exception as e:
            print(f"Error processing {trade['symbol']}: {e}")


if __name__ == "__main__":
    api_resp = get_data_from_wazirx()
    notify_price_increase(api_resp)
