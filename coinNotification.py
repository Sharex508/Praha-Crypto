import psycopg2
import time
import requests
from notifications import notisend
from decimal import Decimal, InvalidOperation, getcontext
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)

# Set decimal precision
getcontext().prec = 6  # Adjust precision as needed

def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database.
    """
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="Harsha508",
            host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
            port="5432",
            database="HarshaCry"
        )
        return connection, connection.cursor()
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None, None

def get_active_trades():
    """
    Fetch all active trades (status = '1') from the 'trading' table.
    Dynamically retrieves column names to ensure correct mapping.
    """
    connection, cursor = get_db_connection()
    if not connection or not cursor:
        return []
    try:
        sql = "SELECT * FROM trading WHERE status = '1'"
        cursor.execute(sql)
        results = cursor.fetchall()
        # Dynamically get column names
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, obj)) for obj in results]
        return data
    except Exception as e:
        logging.error(f"Error fetching active trades: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def get_data_from_binance(filter='USDT'):
    """
    Fetch current prices from Binance API filtered by the specified symbol.
    """
    try:
        response = requests.get('https://api.binance.com/api/v3/ticker/price')
        response.raise_for_status()
        data = response.json()
        filtered_data = [d for d in data if filter in d['symbol'] and 'price' in d]
        logging.info(f"Retrieved {len(filtered_data)} symbols from Binance API.")
        return filtered_data
    except Exception as e:
        logging.error(f"Error fetching data from Binance API: {e}")
        return []

def update_high_price(symbol, new_high_price):
    """
    Update the high price and reset the decrease notification percentage for the given symbol.
    """
    connection, cursor = get_db_connection()
    if not connection or not cursor:
        return
    try:
        sql = "UPDATE trading SET highprice = %s, last_notified_decrease_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (new_high_price, '0.0', symbol))
        connection.commit()
        logging.info(f"Updated high price for {symbol} to {new_high_price} and reset decrease notifications.")
    except Exception as e:
        logging.error(f"Error updating highprice for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_notified_percentage(symbol, percentage):
    """
    Update the last notified percentage for price increases for the given symbol.
    """
    connection, cursor = get_db_connection()
    if not connection or not cursor:
        return
    try:
        sql = "UPDATE trading SET last_notified_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (str(percentage), symbol))  # Pass percentage as string to match TEXT type
        connection.commit()
        logging.info(f"Updated last_notified_percentage for {symbol} to {percentage}%.")
    except Exception as e:
        logging.error(f"Error updating last_notified_percentage for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def update_notified_decrease_percentage(symbol, percentage):
    """
    Update the last notified percentage for price decreases for the given symbol.
    """
    connection, cursor = get_db_connection()
    if not connection or not cursor:
        return
    try:
        sql = "UPDATE trading SET last_notified_decrease_percentage = %s WHERE symbol = %s"
        cursor.execute(sql, (str(percentage), symbol))  # Pass percentage as string to match TEXT type
        connection.commit()
        logging.info(f"Updated last_notified_decrease_percentage for {symbol} to {percentage}%.")
    except Exception as e:
        logging.error(f"Error updating last_notified_decrease_percentage for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def send_notification_grouped(notification_time, increased_coins, decreased_coins):
    """
    Compose and send a grouped notification message.
    """
    message_lines = [f"Time: {notification_time}"]
    
    if increased_coins:
        message_lines.append("\nIncreased Coins:")
        for coin in increased_coins:
            message_lines.append(f"{coin['symbol']}, Purchase Price: {coin['purchase_price']}, Percentage Increase: {coin['percentage_change']:.2f}%")
    
    if decreased_coins:
        message_lines.append("\nDecreased Coins:")
        for coin in decreased_coins:
            message_lines.append(f"{coin['symbol']}, Purchase Price: {coin['purchase_price']}, Percentage Decrease: {coin['percentage_change']:.2f}%")
    
    message = "\n".join(message_lines)
    notisend(message)
    logging.info(f"Notification sent:\n{message}")

def notify_price_increase(api_resp):
    """
    Process active trades, categorize them into increased and decreased coins,
    and send a formatted notification via Telegram.
    """
    db_resp = get_active_trades()

    if not db_resp:
        logging.info("No active trades to process.")
        return

    increased_coins = []
    decreased_coins = []

    notification_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for trade in db_resp:
        try:
            symbol = trade['symbol']
            logging.info(f"Processing {symbol}")

            # Convert initial_price
            initial_price_str = trade.get('purchaseprice') or trade.get('intialprice') or ''
            if not initial_price_str:
                logging.error(f"Invalid initial price for {symbol}: '{initial_price_str}'. Skipping.")
                continue  # Skip to the next trade
            try:
                initial_price = Decimal(initial_price_str)
            except (InvalidOperation, TypeError) as e:
                logging.error(f"Invalid initial price for {symbol}: '{initial_price_str}'. Skipping.")
                continue  # Skip to the next trade

            # Convert last_notified_inc
            last_notified_inc_str = trade.get('last_notified_percentage') or '0.0'
            try:
                last_notified_inc = Decimal(str(last_notified_inc_str))
            except (InvalidOperation, TypeError) as e:
                logging.error(f"Invalid last_notified_percentage for {symbol}: '{last_notified_inc_str}'. Setting to 0.0")
                last_notified_inc = Decimal('0.0')

            # Convert last_notified_dec
            last_notified_dec_str = trade.get('last_notified_decrease_percentage') or '0.0'
            try:
                last_notified_dec = Decimal(str(last_notified_dec_str))
            except (InvalidOperation, TypeError) as e:
                logging.error(f"Invalid last_notified_decrease_percentage for {symbol}: '{last_notified_dec_str}'. Setting to 0.0")
                last_notified_dec = Decimal('0.0')

            # Convert high_price
            high_price_str = trade.get('highprice') or initial_price_str
            try:
                high_price = Decimal(high_price_str)
            except (InvalidOperation, TypeError) as e:
                logging.error(f"Invalid high price for {symbol}: '{high_price_str}'. Using initial price.")
                high_price = initial_price

            # Match API data
            matching_api_data = next((item for item in api_resp if item["symbol"] == symbol), None)
            if matching_api_data:
                current_price_str = matching_api_data['price']
                try:
                    current_price = Decimal(current_price_str)
                except (InvalidOperation, TypeError) as e:
                    logging.error(f"Invalid current price for {symbol}: '{current_price_str}'. Skipping.")
                    continue  # Skip to the next trade

                # Calculate percentage increase
                if initial_price == 0:
                    logging.error(f"Initial price for {symbol} is zero. Cannot calculate percentage change. Skipping.")
                    continue
                percentage_change = ((current_price - initial_price) / initial_price) * Decimal('100')

                # Update high price if current price exceeds it
                if current_price > high_price:
                    update_high_price(symbol, current_price_str)
                    high_price = current_price  # Update in variable too
                    last_notified_dec = Decimal('0.0')  # Reset decrease notifications

                # For increase notifications at every 5% increment
                next_increment = ((last_notified_inc // Decimal('5')) + 1) * Decimal('5')
                if percentage_change >= next_increment:
                    increased_coins.append({
                        'symbol': symbol,
                        'purchase_price': initial_price,
                        'percentage_change': percentage_change
                    })
                    update_notified_percentage(symbol, float(next_increment))
                    last_notified_inc = next_increment  # Update local variable

                # Calculate percentage decrease from high price
                if high_price > 0:
                    percentage_decrease_from_high = ((high_price - current_price) / high_price) * Decimal('100')
                else:
                    percentage_decrease_from_high = Decimal('0.0')

                # For decrease notifications at every 5% decrement
                next_decrement = ((last_notified_dec // Decimal('5')) + 1) * Decimal('5')
                if percentage_decrease_from_high >= next_decrement and percentage_decrease_from_high >= Decimal('5'):
                    decreased_coins.append({
                        'symbol': symbol,
                        'purchase_price': initial_price,
                        'percentage_change': percentage_decrease_from_high
                    })
                    update_notified_decrease_percentage(symbol, float(next_decrement))
                    last_notified_dec = next_decrement  # Update local variable

            else:
                logging.warning(f"No matching API data for {symbol}")
        except Exception as e:
            logging.error(f"Error processing {symbol}: {e}")

    # Compose and send the notification message
    if increased_coins or decreased_coins:
        send_notification_grouped(notification_time, increased_coins, decreased_coins)
    else:
        logging.info("No significant price changes to notify.")

def main():
    while True:
        try:
            # Track the start of the notification iteration
            iteration_start = time.time()
            logging.info(f"Starting notification iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Fetch the latest API response to use for notifications
            api_resp = get_data_from_binance()  # Fetch the latest data

            # Execute the notification logic
            notify_price_increase(api_resp)

            # Measure and print total iteration time
            iteration_end = time.time()
            logging.info(f"Notification iteration completed in {iteration_end - iteration_start:.2f} seconds")

            # Wait for desired interval before the next execution cycle
            time.sleep(60)  # Sleep for 5 minutes (adjust as needed)

        except Exception as e:
            logging.error(f"An error occurred in notification_exe.py: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
