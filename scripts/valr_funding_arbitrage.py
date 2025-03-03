#*********************************
# VALR Funding Arbitrage
#*********************************

"""
TODO:
    - storing historical trades info (executed trades info) - get historical trades instead of storing.
    - create function to kill all positions.
    - Position entering phase / Position closing phase.
    - place limit order at the mid price - to increase chances of fast execution.
    - Add feature to check BTC amount and BTCUSDTPERP position - these should be equal at all times.
    - Add functionality to continue the process even when there are no messages coming through.
        - e.g. update orders for better chance of executing (update stale orders)
"""

#%% LIBRARIES
import pandas as pd
import time
import json
import asyncio
import websockets
import requests
import hashlib
import hmac
from configparser import ConfigParser
from valr_python import Client
from valr_python.exceptions import IncompleteOrderWarning, RESTAPIException
from decimal import Decimal


#%% SECURITY CREDENTIALS
conf_file = 'algo_trading.cfg'
config = ConfigParser()
config.read(conf_file)

API_KEY = config.get('valr_funding_arb', 'api_key')
API_SECRET = config.get('valr_funding_arb', 'api_secret')

#%% INPUTS
WSS_URL = 'wss://api.valr.com/ws/account'

#%% GLOBAL VARIABLES
usdt_initial_bal = None
iteration_no = None
phase = -1 #  (-1="decumulating", 0="holding", 1="accumulating")

#%% CONNECT TO API
valrClient = Client(api_key=API_KEY, 
                    api_secret=API_SECRET)


#%% FUNCTIONS

def sign_request(api_key_secret, timestamp, verb, path, body = ""):
    """Signs the request payload using the api key secret
    api_key_secret - the api key secret
    timestamp - the unix timestamp of this request e.g. int(time.time()*1000)
    verb - Http verb - GET, POST, PUT or DELETE
    path - path excluding host name, e.g. '/v1/withdraw
    body - http request body as a string, optional
    """
    payload = "{}{}{}{}".format(timestamp,verb.upper(),path,body)
    message = bytearray(payload,'utf-8')
    signature = hmac.new( bytearray(api_key_secret,'utf-8'), message, digestmod=hashlib.sha512).hexdigest()
    return signature

def get_valr_headers(api_key, api_secret, path='/ws/account'):
    valr_headers = {}
    
    timestamp = int(time.time() * 1000)
    valr_headers["X-VALR-API-KEY"] = api_key
    valr_headers["X-VALR-SIGNATURE"] = sign_request(api_key_secret=API_SECRET, timestamp=timestamp,
                                                     verb='GET', path=path)
    valr_headers["X-VALR-TIMESTAMP"] = str(timestamp)  # str or byte req for request headers
    
    return valr_headers

# get bid-ask
def get_bid_ask(pair='BTCUSDT'):
    order_book = valrClient.get_order_book_public(currency_pair='BTCUSDT')
    ask = float(order_book['Asks'][0]['price'])
    bid = float(order_book['Bids'][0]['price'])
    
    return bid, ask

# place limit order
def place_limit_order(pair, side, price, size, client_order_id=None):
    
    # Place limit order on perpetual futures
    limit_order = {
         "side": side.upper(),
         "quantity": str(size),
         "price": str(price),
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
        
    order = valrClient.get_order_status(pair, order_id=order_id)
    #print(f"Order status: \n {order}")
    
    return order

def post_market_order(pair, side, size, client_order_id=None):
    
    # Place limit order on perpetual futures
    market_order = {
         "side": side.upper(),
         "base_amount": str(size),
         "pair": pair
    }
    
    try:
        res = valrClient.post_market_order(**market_order)
        order_id = res['id']
    except IncompleteOrderWarning as w:  # HTTP 202 Accepted handling for incomplete orders
        order_id = w.data['id']
        order = valrClient.get_order_status(pair, order_id=order_id)
        print(f"Order status: {order}")
        return order
    except Exception as e:
        print(e)
        # Note! nothing returned if this exception runs.
        return None
    
async def cancel_stale_orders(max_age_seconds=30):
    """Cancel orders that have been open for more than max_age_seconds."""
    
    print("CANCEL STALE ORDERS FUNCTION ...")
    try:
        open_orders = valrClient.get_all_open_orders()
        bid , ask = get_bid_ask()
        
        for order in open_orders:
            if ask < float(order['price'])
        
        if len(open_orders) == 0:
            # Place an order
            if phase == 1:
                if usdt_avail_bal >= trade_amt:
                    # Place limit order buy
                    bid , ask = get_bid_ask()
                    mid_price = round((bid + ask) / 2,0)
                    limit_order = place_limit_order(pair='BTCUSDT', side='buy', price=mid_price, size=0.0001)
            if phase == -1:
                 if btc_avail_bal >= trade_qty:
                     # Place limit order buy
                     bid , ask = get_bid_ask()
                     mid_price = round((bid + ask) / 2,0)
                     limit_order = place_limit_order(pair='BTCUSDT', side='sell', price=mid_price, size=0.0001)
        
    except Exception as e:
        print(f'Error cancelling stale orders: {e}')
    
def handle_message(msg_raw):
    global usdt_avail_bal
    global btc_avail_bal
    global trade_amt
    global trade_qty
    global open_orders
    global usdt_initial_bal
    global phase
    
    msg = json.loads(msg_raw) # parse a valid JSON string and convert it into a Python dictionary
    print(msg['type'])
    
    # HANDLE OPEN_ORDERS_UPDATE
    if msg['type'] == 'OPEN_ORDERS_UPDATE':
        open_orders = msg['data']
        
        # Close order if its been active for more than a 30 seconds and has not executed
    
    # HANDLE BALANCE_UPDATE
    # Check that balance is enough
    if msg['type'] == 'BALANCE_UPDATE':
        data = msg['data']
        symbol = data['currency']['symbol']
        avail_bal = float(data['available'])
        total_bal = float(data['total'])
        print(f'{data["updatedAt"]} | {symbol} Available Balance: {avail_bal}')
        
        if symbol == 'USDT':
            usdt_avail_bal = avail_bal
            
            if usdt_initial_bal is None:
                usdt_initial_bal = usdt_avail_bal
                
        elif symbol =='BTC':
            btc_avail_bal = avail_bal
            
            if total_bal == 0:
                # Switch to accumulating mode/phase
                print(f'SWITCHING TO ACCUMULATING MODE!') # TODO: PROBLEM: This will part will run even when starting a new process. because btc will be zero.
                #phase = 1
        
        # Only run limt buy orders on BALANCE_UPDATE
        if phase == 1: # Switch off when the system is in the closing positions phase
            if usdt_avail_bal >= trade_amt and len(open_orders) == 0 and symbol=='USDT':
                # Place limit order buy
                bid , ask = get_bid_ask()
                mid_price = round((bid + ask) / 2,0)
                limit_order = place_limit_order(pair='BTCUSDT', side='buy', price=mid_price, size=0.0001)
                
                
        if phase == -1:
             if btc_avail_bal >= trade_qty and len(open_orders) == 0 and symbol=='USDT':
                 # Place limit order buy
                 bid , ask = get_bid_ask()
                 mid_price = round((bid + ask) / 2,0)
                 limit_order = place_limit_order(pair='BTCUSDT', side='sell', price=mid_price, size=0.0001)
                 
                 # SWITCH TO ACCUMULATING IF BTC TOTAL BALANCE IS EQUAL TO ZERO
    
    # HANDLE ORDER_STATUS_UPDATE
    # Enter Short Futures Position if Spot Order is Filled.
    if msg['type'] == 'ORDER_STATUS_UPDATE':
        data = msg.get('data',{})
        print(f'{data["orderUpdatedAt"]} | Order {data["orderStatusType"]}: buy {data["originalQuantity"]} {data["currencyPair"]} at {data["executedPrice"]}.')
        
        if data.get('orderStatusType','') == 'Filled' and data.get('currencyPair','') == 'BTCUSDT' and data.get('orderSide','') == 'buy':
            # Place short futures position of similar size
            order = post_market_order(pair='BTCUSDTPERP', side='sell', size=data['originalQuantity'])
            
    # Close Short Futures Position        
        if data.get('orderStatusType','') == 'Filled' and data.get('currencyPair','') == 'BTCUSDT' and data.get('orderSide','') == 'sell':
            # Place short futures position of similar size
            order = post_market_order(pair='BTCUSDTPERP', side='buy', size=data['originalQuantity'])
    
    print('-'*55)
            
        
usdt_avail_bal = 0
trade_amt = 9 #TODO: This is a hardcoded trade amount and will need to be updated to a real time one.
trade_qty = 0.0001
async def stream_valr_account():
    
    
    async def periodic_tasks():
        """Runs periodic tasks like canceling stale orders."""
        while True:
            await asyncio.sleep(30)  # Runs every 30 seconds
            await cancel_stale_orders()
    
    while True:
        try:
            async with websockets.connect(WSS_URL, extra_headers=get_valr_headers(API_KEY, API_SECRET)) as ws:
                print('Websocket Connection Established!')
                
                # Start periodic task in background
                asyncio.create_task(periodic_tasks())
                
                while True:
                    msg_raw = await ws.recv() # Waits for WebSocket message (blocking)
                    #print(msg_raw)
                    handle_message(msg_raw)
                    
                    # Check every 30 seconds and run some code.
                    if phase == 1: # Switch off when the system is in the closing positions phase
                        if usdt_avail_bal >= trade_amt and len(open_orders) == 0:
                            # Place limit order buy
                            bid , ask = get_bid_ask()
                            mid_price = round((bid + ask) / 2,0)
                            limit_order = place_limit_order(pair='BTCUSDT', side='buy', price=mid_price, size=0.0001)
                            
                    #print('DEBUG | Sleeping for 0.2 seconds ...')       
                    # await asyncio.sleep(1)
                    # Will this code be blocking.
                    # Will other handle message code be run whilst this block of code is calculating or performing I/O operations?
                    # Do the messages get queued for exection when this block of code is done. - there will be potential...
                    # ... splippage for a Filled order event, where the corresponding futures trade is delayed.
                    
        except Exception as e:
            # Handle Errors & Re-connect
            print(f"Error: {e}")


#%% MAIN

if __name__ == '__main__':
    try:
        asyncio.run(stream_valr_account())
    except KeyboardInterrupt:
        print("Shutting down WebSocket connection gracefully.")
        print(f'Initial USDT Balance: {usdt_initial_bal}')
        # Initial Available USDT
        # Initial Total USDT
        # Initial Reserved USDT 


if False:
    bid = get_bid_ask()[0]
    ask = get_bid_ask()[1]
    order = place_limit_order(pair='BTCUSDT', side='buy', price=bid, size=0.0001)
    
    # Sell off some BTC
    ask = get_bid_ask()[1]
    order = place_limit_order(pair='BTCUSDT', side='sell', price=ask, size=0.0001)
    
if False:
    spot_trades = pd.DataFrame(valrClient.get_trade_history('BTCUSDT'))
    futures_trades = pd.DataFrame(valrClient.get_trade_history('BTCUSDTPERP'))
    
    trades = pd.concat([spot_trades, futures_trades])
    
    trades['time'] = pd.to_datetime(trades['tradedAt'])
    trades['pair'] = trades['currencyPair']
    trades['price'] = pd.to_numeric(trades['price'])
    trades['quantity'] = pd.to_numeric(trades['quantity'])
    
    trades = trades[['time', 'pair', 'side', 'price', 'quantity', 'fee']]
    
if False:
    url = "https://api.valr.com/v1/positions/funding/history?currencyPair=BTCUSDTPERP&skip=0&limit=100"
    
    payload={}
    response = requests.request("GET", url, headers=get_valr_headers(API_KEY, API_SECRET, path='/v1/positions/funding/history?currencyPair=BTCUSDTPERP&skip=0&limit=100'), data=payload)
    
    response = json.loads(response.text)
    df_funding = pd.DataFrame(response)
    
    avg_funding_rate = df_funding['fundingRate'].mean()
    total_funding_amt = df_funding['fundingAmount'].sum()
    print(f'Average Funding Rate: {avg_funding_rate}')
        
    
    
    
    
    

    
    
