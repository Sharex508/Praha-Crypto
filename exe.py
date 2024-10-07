from Buy import get_diff_of_db_api_values, update_last_prices, get_data_from_wazirx
from coinNotification import notify_price_increase
import time

def main():
    while True:
        try:
            # Log start time for tracking
            start_time = time.time()
            print(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Step 1: Fetch the latest price data from Binance API
            api_resp = get_data_from_wazirx()

            # Step 2: Execute purchasing logic from Buy.py
            get_diff_of_db_api_values(api_resp)  # Compare prices, handle purchases

            # Step 3: Update last prices in the database from Buy.py
            update_last_prices(api_resp)  # Update prices in the database

            # Step 4: Execute notification logic from coinNotification.py
            notify_price_increase(api_resp)  # Send notifications for significant price changes

            # Log end time for tracking and print elapsed time
            end_time = time.time()
            print(f"Iteration completed in {end_time - start_time:.2f} seconds")

            # Wait for 10 seconds before the next iteration
            time.sleep(10)
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
