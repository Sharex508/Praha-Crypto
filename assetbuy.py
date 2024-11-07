from binance.spot import Spot as Client
from notifications import notisend  # Ensure this module exists and is properly configured

# Initialize Binance client with API keys
API_KEY = '8DQevqOGL7FE8rrzvOrBn6brernCBHqfakSeN8Mv2n7a8V5gvo21CyDfmltpHpmP'
API_SECRET = 'epeo2Y25uFPovRqM1fRwoHCGcReGJcrk4tgb0bophDK0v7HMItadf1w84EtmiRlO'
client = Client(API_KEY, API_SECRET)

def buy_asset_with_usd(asset_symbol, usd_amount):
    """
    Purchase an amount of asset specified in USD using Binance Spot API.
    
    Parameters:
    asset_symbol (str): The symbol of the asset to buy (e.g., 'NEIROUSDT').
    usd_amount (float): The USD amount to spend on the purchase, assuming USD ≈ USDT.
    """
    # Convert USD amount to USDT amount, assuming 1 USD ≈ 1 USDT
    usdt_amount = usd_amount  # This example assumes 1 USD ≈ 1 USDT
    
    # Specify the market pair (e.g., NEIROUSDT)
    asset_pair = asset_symbol 
    
    try:
        # Create a market buy order by specifying the amount of USDT to spend
        # Note: For the python-binance Spot client, use new_order with type=MARKET
        order = client.new_order(
            symbol=asset_pair,
            side='BUY',
            type='MARKET',
            quoteOrderQty=str(usdt_amount)  # Convert to string as required by the API
        )
        success_message = f"Successfully purchased {asset_symbol} worth ${usd_amount} USD."
        print(success_message)
        notisend(success_message)  # Send notification
    except Exception as e:
        error_message = f"An error occurred while purchasing {asset_symbol}: {e}"
        print(error_message)
        notisend(error_message)  # Send error notification

# Example: Buy $5 worth of NEIRO
buy_asset_with_usd('NEIROUSDT', 5)
