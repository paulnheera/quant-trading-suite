#***************************
# Luno Market Making Bot
#***************************

"""
- How many bid-ask trades should I place at a time for a single pair.
- How does this market maker perform in its current form?
- Add error handling
- Add websocket stream to listen to when orders are filled. (for logging info)
"""

import time
import requests
from configparser import ConfigParser
from luno_python.client import Client as LunoClient
from luno_python.error import APIError


#%% Configurations
conf_file = 'algo_trading.cfg'
config = ConfigParser()
config.read(conf_file)

API_KEY = config.get('luno', 'api_key')
API_SECRET = config.get('luno', 'api_secret')

#%% Connect to APIs
lunoClient = LunoClient(api_key_id=API_KEY, 
                    api_key_secret=API_SECRET)

#%%

PAIR = "SOLZAR"
SPREAD = 0.01 #  spread = tc * 2 + tp ( 0.004 * 2 + 0.002)
ORDER_SIZE = 0.05 # 0.02 SOL per order
UPDATE_INTERVAL = 2*60*60 # Adjust orders every 5 seconds

#%%

def get_order_book():
    """ Fetches the order book from the exchange. """
    order_book = lunoClient.get_order_book(pair=PAIR)
    best_bid = float(order_book['bids'][0]['price'])
    best_ask = float(order_book['asks'][0]['price'])
    
    return {'best_bid':best_bid, 'best_ask':best_ask}

def place_order(side, price, size):
    
    return lunoClient.post_limit_order(pair=PAIR, price=price, type=side, volume=size)
    
    
def cancel_order(order_id):
    """Cancels an existing order."""
    response = lunoClient.stop_order(order_id)
    

def market_making_bot():
    print("Starting Luno Market Maker Bot...")
    
    
    while True:
        try:
            # Step 1: Get best bid and ask prices
            best_bid, best_ask = get_order_book().values()
            mid_price = (best_bid + best_ask) / 2
            
            # Step 2: Determine new bid and ask prices
            bid_price = round(mid_price * (1 - SPREAD), 2)
            ask_price = round(mid_price * (1 + SPREAD), 2)
            
            # Step 3: Place new limit orders
            bid_order = place_order("BUY", bid_price, ORDER_SIZE)
            ask_order = place_order("SELL", ask_price, ORDER_SIZE)
            
            print(f"Placed orders: Buy @ {bid_price}, Sell @ {ask_price}")
            
            # Step 4: Wait and then cancel unfilled orders
            time.sleep(UPDATE_INTERVAL)
            cancel_order(bid_order["order_id"])
            cancel_order(ask_order["order_id"])
            
        except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)  # Retry after a short delay
                
# Run the bot
market_making_bot()
                
                
            
            
    
