# notification_exe.py

from coinNotification import notify_price_increase
from Buy import get_data_from_binance  # Import get_data_from_wazirx from Buy.py
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

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
            time.sleep(300)  # Sleep for 5 minutes (adjust as needed)

        except Exception as e:
            logging.error(f"An error occurred in notification_exe.py: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
