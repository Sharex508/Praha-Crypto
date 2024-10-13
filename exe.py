from Buy import get_diff_of_db_api_values, update_last_prices, get_data_from_wazirx
from coinNotification import notify_price_increase
import time

def main():
    while True:
        try:
            # Track the start of the entire iteration
            iteration_start = time.time()
            print(f"Starting new iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Group 1: API Fetching + Buy Logic
            group1_start = time.time()

            # Step 1: Fetch the latest price data
            api_resp = get_data_from_wazirx()

            # Step 2: Handle purchasing logic
            get_diff_of_db_api_values(api_resp)  # Perform price comparisons and decide on purchases

            # Step 3: Update last prices in the database
            update_last_prices(api_resp)  # Update the DB with the latest prices
            
            group1_end = time.time()
            print(f"Group 1 (API Fetch + Buy Logic) completed in {group1_end - group1_start:.2f} seconds")

            # Group 2: Notification Logic
            group2_start = time.time()
            
            # Step 4: Trigger notifications for price increases or decreases
            notify_price_increase(api_resp)  # Notify if there are significant price changes

            group2_end = time.time()
            print(f"Group 2 (Notification Logic) completed in {group2_end - group2_start:.2f} seconds")

            # Measure and print total iteration time
            iteration_end = time.time()
            print(f"Iteration completed in {iteration_end - iteration_start:.2f} seconds")

            # Wait for 10 seconds before the next execution cycle
            time.sleep(10)

        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(300)

if __name__ == "__main__":
    main()
