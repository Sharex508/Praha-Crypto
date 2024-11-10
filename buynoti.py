# notifications.py
import requests
import logging
import os

def notisend(message):
    """
    Send a message via Telegram.
    
    Parameters:
    message (str): The message to send.
    """
    BOT_TOKEN = '7301531753:AAFZO8f90R1U6Of2RJKD1j00nBp90TRfi00'      # Set this as an environment variable
    CHAT_ID =  '1893850031'       # Set this as an environment variable
    
    if not BOT_TOKEN or not CHAT_ID:
        logging.error("Telegram BOT_TOKEN or CHAT_ID not set.")
        return
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            logging.error(f"Failed to send message: {response.text}")
    except Exception as e:
        logging.error(f"Exception occurred while sending message: {e}")
