# buy_exe.py

from Buy import show
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def main():
    while True:
        try:
            # Track the start of the buy iteration
            iteration_start = time.time()
            logging.info(f"Starting buy iteration at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Execute the buy logic
            show()  # This will run the purchasing logic defined in Buy.py

            # Measure and print total iteration time
            iteration_end = time.time()
            logging.info(f"Buy iteration completed in {iteration_end - iteration_start:.2f} seconds")

            # Wait for desired interval before the next execution cycle
            time.sleep(10)  # Sleep for 5 minutes (adjust as needed)

        except Exception as e:
            logging.error(f"An error occurred in buy_exe.py: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
