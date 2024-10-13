import psycopg2
import requests
from notifications import notisend

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
        # Ensure the correct columns are fetched, remove 'quantity' if not required
        sql = """
        SELECT symbol, intialPrice, highPrice, lastPrice, margin3, margin5, 
               margin10, margin20, purchasePrice, created_at, status, 
               COALESCE(last_notified_percentage, 0.0) as last_notified_percentage
        FROM trading WHERE status = '1'
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        keys = ('symbol', 'intialPrice', 'highPrice', 'lastPrice', 'margin3', 'margin5', 
                'margin10', 'margin20', 'purchasePrice', 'created_at', 'status', 'last_notified_percentage')
        data = [dict(zip(keys, obj)) for obj in results]
        return data
    except Exception as e:
        print(f"Error fetching active trades: {e}")
        return []  # Return empty list if there's an error
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
        sql = "UPDATE trading SET highPrice = %s WHERE symbol = %s"
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

def notify_price_increase(api_resp):
    db_resp = get_active_trades()

    if not db_resp:
        print("No active trades found or error in fetching trades.")
        return

    for trade in db_resp:
        try:
            symbol = trade['symbol']
            initial_price = float(trade['intialPrice'])
            last_notified = float(trade['last_notified_percentage'])
            high_price = float(trade['highPrice'])

            matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
            if matching_api_data:
                current_price = float(matching_api_data['price'])
                percentage_increase = ((current_price - initial_price) / initial_price) * 100

                # Update high price if current price exceeds it
                if current_price > high_price:
                    update_high_price(symbol, current_price)

                # Check for a 10% or more increase since the last notification
                if percentage_increase >= last_notified + 10:
                    send_notification(symbol, initial_price, current_price, "increase", percentage_increase)
                    update_notified_percentage(symbol, percentage_increase)

                # Check for a 5% decrease from the highest gain
                gain = high_price - initial_price
                decrease_threshold = high_price - (gain * 0.05)
                if current_price <= decrease_threshold:
                    send_notification(symbol, initial_price, current_price, "decrease", -5)
                    update_notified_percentage(symbol, percentage_increase)

        except TypeError as e:
            print(f"TypeError encountered for {trade['symbol']}: {e}. Skipping.")
        except Exception as e:
            print(f"Error processing {trade['symbol']}: {e}")

if __name__ == "__main__":
    api_resp = get_data_from_wazirx()
    notify_price_increase(api_resp)
