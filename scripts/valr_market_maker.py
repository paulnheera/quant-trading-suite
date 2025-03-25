# ***********************
# VALR Market Maker
# ***********************


"""
To Do:
    - stream order-book and place orders when spread widens.
    - * we can't to the above as the websocket does not have a stream for other pairs other than BTCZAR & ETHZAR
    - we might be able to stream this data via the "wss://api.valr.com/ws/trade" websocket
    - update my bid/ask if it is no longer the best.
    -- RESTAPIException: (200, 'valr-python: unknown API error. HTTP (200): Expecting value')
    -- Improve use threading to place orders at the same time.

"""

#%% Import Libraries
import time
import requests
from configparser import ConfigParser
from luno_python.client import Client as LunoClient
from luno_python.error import APIError
from valr_python import Client
from valr_python.exceptions import IncompleteOrderWarning, RESTAPIException
from decimal import Decimal

#%% Configurations
conf_file = 'algo_trading.cfg'
config = ConfigParser()
config.read(conf_file)

API_KEY = config.get('valr_market_maker', 'api_key')
API_SECRET = config.get('valr_market_maker', 'api_secret')

#%% Connect to APIs
valrClient = Client(api_key=API_KEY, 
                    api_secret=API_SECRET)

#%% Inputs
PAIR = "BTCUSDT"
SPREAD = 0.0016 #  spread = tc * 2 + tp ( 0.004 * 2 + 0.002)
ORDER_SIZE = 0.0003 # 0.02 SOL per order
UPDATE_INTERVAL = 20*60 # 20 minutes

#%% Verify Connection
#print(valrClient.get_balances())

#%% Functions

def get_order_book(pair):
    """ Fetches the order book from the exchange. """
    order_book = valrClient.get_order_book_public(currency_pair=pair)
    best_bid = float(order_book['Bids'][0]['price'])
    best_ask = float(order_book['Asks'][0]['price'])
    spread = (best_ask/best_bid) - 1
    
    return {'best_bid':best_bid, 'best_ask':best_ask, 'spread':spread}


def place_order(pair, side, price, size, client_order_id=None):
    
    # Place limit order on perpetual futures
    limit_order = {
         "side": side.upper(),
         "quantity": Decimal(size),
         "price": Decimal(str(price)),
         "pair": pair,
         "post_only": True,
    }
    
    try:
        res = valrClient.post_limit_order(**limit_order)
        order_id = res['id']
        print(order_id)
    except IncompleteOrderWarning as w:  # HTTP 202 Accepted handling for incomplete orders
        order_id = w.data['id']
        print(order_id)
    except Exception as e:
        print(e)
        
    order = valrClient.get_order_status(PAIR, order_id=order_id)
    print(f"Order status: {order}")
    
    return order

def cancel_order(pair, order_id):
    """Cancels an existing order."""
    try:
        response = valrClient.delete_order(currency_pair=pair, order_id=order_id)
    except RESTAPIException as e:
        if e.status_code == 200:
            pass

    print(f"Cancelled order: {order_id}")


def market_making_bot():
    print("Starting Luno Market Maker Bot...")
    
    order_counter = 1
    
    while True:
        try:
            # Step 1: Get best bid and ask prices
            best_bid, best_ask, spread = get_order_book(pair=PAIR).values()
            mid_price = (best_bid + best_ask) / 2
            print(f"Best bid: {best_bid}")
            print(f"Best ask: {best_ask}")
            print(f"Spread: {spread}")
            
            if spread > 0.0007:
                # Step 2: Determine new bid and ask prices
                bid_price = round(mid_price * (1 - SPREAD)) #TODO: make the rounding number dynamic.
                ask_price = round(mid_price * (1 + SPREAD))
                print(f"Bid price: {bid_price}")
                print(f"Ask price: {ask_price}")
                
                # Step 3: Place new limit orders
                bid_order = place_order(pair=PAIR, side="BUY", price=bid_price, size=ORDER_SIZE)
                ask_order = place_order(pair=PAIR, side="SELL",price=ask_price, size=ORDER_SIZE)
                
                if bid_order['orderStatusType'] == 'Failed':
                    cancel_order(pair=PAIR, order_id=ask_order['orderId'])
                
                if ask_order['orderStatusType'] == 'Failed':
                    cancel_order(pair=PAIR, order_id=bid_order['orderId'])
                    
                order_counter += 1
            
                print(f"Placed orders: Buy @ {bid_price}, Sell @ {ask_price}")
            
                # Step 4: Wait and then cancel unfilled orders
                time.sleep(UPDATE_INTERVAL)
                cancel_order(pair=PAIR, order_id=bid_order['orderId'])
                cancel_order(pair=PAIR, order_id=ask_order['orderId'])
            else:
                time.sleep(2)
                
        except Exception as e:
                print(f"Error: {e}")
                time.sleep(3)  # Retry after a short delay
                
# Run the bot
market_making_bot()

    
   
    

