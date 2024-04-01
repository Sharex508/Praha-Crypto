

from binance.spot import Spot as Client

# Initialize Binance client with API keys
API_KEY = 'Gx5uw7H44XRiT2BdgIgZuPznhILMLhmktmncowAalGPIppqnFyDY401r2xhEgrYf'
API_SECRET = 'gQ4QEl42lFeHvbmMUaOGTbX3p00SPBozZHBUQbZeEJ2A7KKYL5ruYDDCO2ZjeYOV'
client = Client(API_KEY, API_SECRET)

def buy_asset_with_usd(asset_symbol, usd_amount):
    """
    Purchase an amount of asset specified in USD using Binance Spot API.
    
    Parameters:
    asset_symbol (str): The symbol of the asset to buy (e.g., 'HIFI').
    usd_amount (float): The USD amount to spend on the purchase, assuming USD ≈ USDT.
    """
    # Convert USD amount to USDT amount, assuming 1 USD ≈ 1 USDT
    usdt_amount = usd_amount  # This example assumes 1 USD ≈ 1 USDT
    
    # Specify the market pair (e.g., HIFIUSDT)
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
        print(f"Successfully purchased {asset_symbol} worth ${usd_amount} USD.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example: Buy $1 worth of HIFI

