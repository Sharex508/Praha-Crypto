from Buy import get_diff_of_db_api_values, update_last_prices, get_data_from_wazirx
from coinNotification import notify_price_increase
import time

def main():
    while True:
        try:
            start_time = time.time()  # Track the start time for each iteration
            print(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Step 1: Fetch the latest price data
            api_resp = get_data_from_wazirx()  # Fetch current prices from Binance API
            
            # Step 2: Handle purchasing logic (buy.py)
            get_diff_of_db_api_values(api_resp)  # Perform price comparisons, decide on purchases
            
            # Step 3: Update last prices in the database (buy.py)
            update_last_prices(api_resp)  # Update the DB with the latest prices
            
            # Step 4: Trigger notifications for price increases or decreases (coin_notification.py)
            notify_price_increase(api_resp)  # Notify if there are significant price changes

            # Measure and print iteration time
            end_time = time.time()
            iteration_duration = end_time - start_time
            print(f"Iteration completed in {iteration_duration:.2f} seconds")

            # Step 5: Wait for 10 seconds before the next execution cycle
            time.sleep(10)  # Control the loop to run every 10 seconds

        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(300)

if __name__ == "__main__":
    main()
