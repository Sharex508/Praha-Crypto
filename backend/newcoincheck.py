import requests
import psycopg2
import logging
import schedule
import time

logging.basicConfig(level=logging.INFO)

# Database connection parameters
DB_USER = "postgres"
DB_PASSWORD = "Harsha508"
DB_HOST = "prahacrypto08.cf0e8ug6ynu6.ap-south-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_DATABASE = "prahacrypto08"

def get_db_connection():
    return psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_DATABASE
    )

def get_data_from_wazirx(filter='USDT'):
    try:
        data = requests.get('https://api.binance.com/api/v3/ticker/price').json()
        resp = [d for d in data if filter in d['symbol'] and 'price' in d]
        for obj in resp:
            lprice = float(obj['price'])
            obj.update({"lastPrice": lprice})
        return resp
    except Exception as e:
        logging.error(f"Failed to fetch data from Binance: {e}")
        return []

def fetch_existing_symbols():
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT symbol FROM trading")
                existing_symbols = cursor.fetchall()
                return [symbol[0] for symbol in existing_symbols]
    except Exception as e:
        logging.error(f"Failed to fetch existing symbols: {e}")
        return []

def check_and_insert_new_coins():
    existing_symbols = fetch_existing_symbols()
    wazirx_data = get_data_from_wazirx()
    new_coins = [coin for coin in wazirx_data if coin['symbol'] not in existing_symbols]

    if new_coins:
        try:
            with get_db_connection() as connection:
                with connection.cursor() as cursor:
                    for coin in new_coins:
                        cursor.execute(
                            "INSERT INTO trading (symbol, lastPrice, status) VALUES (%s, %s, '0')",
                            (coin['symbol'], coin['lastPrice'])
                        )
                    connection.commit()
                logging.info(f"Inserted {len(new_coins)} new coins.")
        except Exception as e:
            logging.error(f"Failed to insert new coins: {e}")

def job():
    check_and_insert_new_coins()

def main():
    schedule.every().hour.do(job)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
