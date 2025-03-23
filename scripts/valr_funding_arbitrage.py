#*********************************
# VALR Funding Arbitrage
#*********************************

"""
TODO:
    - Position entering phase / Position closing phase.
    - Add feature to check BTC amount and BTCUSDTPERP position - these should be equal at all times.
    - Add functionality to continue the process even when there are no messages coming through. (another async coroutine)
        - e.g. update orders for better chance of executing (update stale orders)
    - Add logic to enter positions when spread has widened
    - Add logic to exit positions when spread has narrowed.
"""

#%% LIBRARIES
import pandas as pd
import numpy as np
import math
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
import aiofiles
from collections import deque

#%% SECURITY CREDENTIALS
conf_file = 'algo_trading.cfg'
config = ConfigParser()
config.read(conf_file)

API_KEY = config.get('valr_funding_arb', 'api_key')
API_SECRET = config.get('valr_funding_arb', 'api_secret')

#%% INPUTS
WSS_URL = 'wss://api.valr.com/ws/account'

SPOT_PAIR = "BTCZAR"
FUT_PAIR = "BTCZARPERP"
QUOTE = 'ZAR'
BASE = 'BTC'
SPREAD_THRESHOLD = 0.0001

#%% GLOBAL VARIABLES
usdt_initial_bal = None
btc_avail_bal = None
btc_total_bal = None
btcperp_total_bal = None 
iteration_no = None
phase = -1 #  (-1="Unwinding", 0="holding", 1="accumulating")
usdt_total_bal = 0
trade_qty = 0.0003
trade_amt = trade_qty * 1600000  #TODO: This is a hardcoded trade amount and will need to be updated to a real time one.
OpenOrders = []

spread = -1
spread_history = deque(maxlen=100) # Keep last 100 spreads
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

def get_bid_ask(pair='BTCUSDT'):
    order_book = valrClient.get_order_book_public(currency_pair='BTCUSDT')
    ask = float(order_book['Asks'][0]['price'])
    bid = float(order_book['Bids'][0]['price'])
    
    return bid, ask

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
    except IncompleteOrderWarning as w:  # HTTP 202 Accepted handling for incomplete orders
        order_id = w.data['id']
    except Exception as e:
        print(e)
        
    order = valrClient.get_order_status(pair, order_id=order_id)
    
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
    
def get_futures_funding(pair):
    """ Calculates the total funding payemnts accumulated so far, and the costs of this cycle."""
    
    url = f"https://api.valr.com/v1/positions/funding/history?currencyPair={pair}&skip=0&limit=100"
    
    payload={}
    response = requests.request("GET", url, headers=get_valr_headers(API_KEY, API_SECRET, path=f'/v1/positions/funding/history?currencyPair={pair}&skip=0&limit=100'), data=payload)
    
    response = json.loads(response.text)
    df_funding = pd.DataFrame(response)
    
    df_funding[['fundingRate', 'fundingAmount', 'positionTotal']] = df_funding[['fundingRate', 'fundingAmount', 'positionTotal']].apply(pd.to_numeric)
    df_funding['createdAt'] = pd.to_datetime(df_funding['createdAt'])
    
    avg_funding_rate = df_funding['fundingRate'].mean()
    tot_funding_rate = df_funding['fundingRate'].sum()
    total_funding_amt = df_funding['fundingAmount'].sum()
    print(f'Average Funding Rate: {round(avg_funding_rate * 100,4)}%')
    print(f'Total Funding Rate: {round(tot_funding_rate*100,4)}%')
    print(f'Total Funding Amount: {round(total_funding_amt,4)}')
    
    return df_funding

def get_open_futures_positions():
    """ Returns the open futures positions for an account """
    
    url = "https://api.valr.com/v1/positions/open"
    path = "/v1/positions/open"
    
    payload={}
    response = requests.request("GET", url, headers=get_valr_headers(API_KEY, API_SECRET, path=path), data=payload)
    
    response = json.loads(response.text)
    df_positions = pd.DataFrame(response)
    
    return df_positions

def get_trades(pair):
    """ """
    df = valrClient.get_trade_history(currency_pair=pair)
    df = pd.DataFrame(df)
    
    df['tradedAt'] = pd.to_datetime(df['tradedAt'])
    df[['price', 'quantity','fee']] = df[['price', 'quantity','fee']].apply(pd.to_numeric)
    
    df = df[['tradedAt', 'orderId','currencyPair', 'side','price', 'quantity', 'fee', 'feeCurrency']]
    
    return df

def get_transactions(currency, start_time, end_time):
    df = valrClient.get_transaction_history()

def round_down(x, n):
    """ Round x down to n decimal places"""
    factor = 10 ** n
    return math.floor(x * factor) / factor

                    
#%% MAIN FUNCTIONS
 
async def entering_positions():
    """
    To exit the function must *return* when
        - all available usdt has been deployed (i.e. capital is used up) and there is a matching futures position.
        - a stopping condition is met (e.g. a max time per phase).
        - an external condition forces switching phase.
    """
    global btc_total_bal
    global usdt_total_bal
    global trade_amt
    global trade_qty
    global OpenOrders
    
    print("*** STARTING ENTERING POSITIONS! ***")
    balances = [bal for bal in valrClient.get_balances() if float(bal['total']) > 0]
    for bal in balances:
        print(f'{bal["currency"]}: {bal["total"]} | Value: {bal["totalInReference"]}')
    
    usdt_total_bal = float([i for i in valrClient.get_balances() if i['currency'] == QUOTE][0]['total'])
    btc_total_bal = float([i for i in valrClient.get_balances() if i['currency'] == BASE][0]['total'])
    # btcperp_total_bal = 0  #float(get_open_futures_positions()['quantity'][0])
    
    async def handle_trade_message(msg_raw):
        global btc_total_bal
        global usdt_total_bal
        global trade_amt
        global trade_qty
        global OpenOrders
        
        global spot_bid
        global spot_ask
        global fut_bid
        global fut_ask
        
        msg = json.loads(msg_raw)
        
        if msg['type'] == 'MARKET_SUMMARY_UPDATE':
            
            if msg.get('currencyPairSymbol','') == SPOT_PAIR:
                spot_bid = float(msg['data']['bidPrice'])
                spot_ask = float(msg['data']['askPrice'])                  
            elif msg.get('currencyPairSymbol','') == FUT_PAIR:
                fut_bid = float(msg['data']['bidPrice'])
                fut_ask = float(msg['data']['askPrice'])
                
            try:
                spread = (fut_bid / spot_bid) - 1
                spread_history.append(spread)
                print(f'Spread: {round(spread*100,4)}%')
            except NameError as e:
                print(e)
            
            # TODO: Only enter if spread is above a certain threshold.
            if usdt_total_bal >= trade_amt and len(OpenOrders) == 0:
                mid_price = round((spot_bid + spot_ask)/2,0) # Calculate mid-price
                print(f'Mid-Price: {mid_price}')
                
                limit_order = place_limit_order(pair=SPOT_PAIR, side='buy', price=mid_price, size=trade_qty)
                OpenOrders.append(limit_order) # CAUION!! - The order might not get placed - due to the order being post only.
            
            try:
                for order in OpenOrders:
                    if float(order['price']) >= spot_ask * (1.001):
                        del_order = valrClient.delete_order(SPOT_PAIR, order_id=order['orderId'])
                        print(f'Cancelled stale order: {order["orderId"]}')
            except Exception as e:
                print(f"Failed to delete order. Error: {e}")

    async def stream_trade(url='wss://api.valr.com/ws/trade'):
        """ Stream the Trade WebSocket and handle the market summary updates """
        global btc_total_bal
        global usdt_total_bal
        global btcperp_total_bal
        global trade_qty
        global trade_amt
        
        backoff = 1  # Initial backoff time for reconnection
        
        while usdt_total_bal >= trade_amt or btcperp_total_bal < round_down(btc_total_bal, 4):
            try:
                async with websockets.connect(url) as websocket:
                    print("[TRADE] Connected to Trade WebSocket.")
                    
                    # Send subscription message
                    ms_sub_mesg = {
                        "type": "SUBSCRIBE", 
                        "subscriptions": [{"event": "MARKET_SUMMARY_UPDATE", "pairs": [SPOT_PAIR, FUT_PAIR]}]
                    }
                    await websocket.send(json.dumps(ms_sub_mesg))
                    print("[TRADE] Subscribed!")
                    
                    async def send_ping():
                        """ Periodically sends PING messages to keep the connection alive."""
                        while True:
                            await asyncio.sleep(30) # Send PING every 30 seconds
                            await websocket.send(json.dumps({'type':'PING'}))
                            # print("📡 Sent PING to WebSocket")
                            
                    # Run PING sender as a background task
                    # asyncio.create_task(send_ping()) 
                    # Note: since send_ping() is tied to the first loop, the task does not persist after the loop exits.

                    while usdt_total_bal >= trade_amt or btcperp_total_bal < round_down(btc_total_bal, 4):
                        msg_raw = await websocket.recv()
                        # print(msg_raw)
                        await handle_trade_message(msg_raw)
                        #await asyncio.sleep(3)

            except websockets.ConnectionClosedError:
                print("[TRADE] Connection closed. Reconnecting in 5 seconds...")   
                await asyncio.sleep(backoff)
            except Exception as e:
                print(f"[TRADE] An error occurred: {e}")
                await asyncio.sleep(backoff)
                
    async def handle_account_message(msg_raw):
        global btc_total_bal
        global usdt_total_bal
        global trade_amt
        global btcperp_total_bal
        global trade_qty
        global OpenOrders
        
        msg = json.loads(msg_raw)
        print(msg['type'])
    
        # Print if limit order has been placed successfully
        
        if msg['type'] == 'OPEN_ORDERS_UPDATE':
            OpenOrders = msg['data']
            print(msg['data'])
        
        # Handle BALANCE_UPDATE (update btc_avail_bal)
        if msg['type'] == 'BALANCE_UPDATE':
            data = msg['data']
            
            if data['currency']['symbol'] == BASE:
                btc_total_bal = float(data['total']) # Update the BTC Total Balance
                print(f'{data["updatedAt"]} | {BASE} Total Balance: {btc_total_bal}')
            if data['currency']['symbol'] == QUOTE:
                usdt_total_bal = float(data['total']) # Update the BTC Total Balance
                print(f'{data["updatedAt"]} | {QUOTE} Total Balance: {usdt_total_bal}')
                
        # If limit buy order is filled --> Place short futures position @ market
        if msg['type'] == 'ORDER_STATUS_UPDATE': 
            data = msg.get('data',{})
            print(f'{data["orderUpdatedAt"]} | Order {data["orderStatusType"]}: buy {data["originalQuantity"]} {data["currencyPair"]} at {data["executedPrice"]}.')
            
            if data.get('orderStatusType','') == 'Failed':
                OpenOrders = []
            
            if data.get('orderStatusType','') == 'Filled' and data.get('currencyPair','') == SPOT_PAIR and data.get('orderSide','') == 'buy':
                # Place short futures position of similar size (i.e. hedge long position)
                market_order = post_market_order(pair=FUT_PAIR, side='sell', size=data['originalQuantity'])
            
        if msg['type'] == 'OPEN_POSITION_UPDATE ':
            data = msg.get('data',{})
            btcperp_total_bal = data['quantity'] ## TODO! Make this operation more robust. What did I mean by more robust here?
            print(f"Futures position updated to: {btcperp_total_bal}")

        if msg['type'] == 'NEW_ACCOUNT_TRADE ':
            data = msg.get('data',{})
            dt = pd.to_datetime(data.get('tradedAt')) # _{dt.strftime("%Y%m%d")}
            async with aiofiles.open(f'data/valr/funding_arb_trades.txt', mode='a') as f:
                await f.write(json.dumps(data) + "\n")            
        
    async def stream_account():
        global btc_total_bal
        global usdt_total_bal
        global trade_qty
        global btcperp_total_bal
        global trade_amt
        
        # Open WebSocket Connection
        while usdt_total_bal >= trade_amt or btcperp_total_bal < round_down(btc_total_bal, 4):
            try:
                async with websockets.connect(WSS_URL, extra_headers=get_valr_headers(API_KEY, API_SECRET)) as ws:
                #async with websockets.connect(WSS_URL, additional_headers=get_valr_headers(API_KEY, API_SECRET)) as ws:
                    print('[ACCOUNT] Websocket Connection Established!')
                    while usdt_total_bal >= trade_amt or btcperp_total_bal < round_down(btc_total_bal, 4):
                        msg_raw = await ws.recv() # Waits for WebSocket message (blocking)
                        await handle_account_message(msg_raw)
                        
            except Exception as e:
                # Handle Errors & Re-connect
                print(f"Error: {e}") 
                    
    # Unsubscribe and Close websocket connection
    
    # Hand over to the start of the main procedure
    
    await asyncio.gather(stream_trade(), stream_account())
    
    # If all BTC is sold, return control to `main()`
    print("*** ENTERING POSITIONS COMPLETE! MOVING TO HOLDING PHASE ...***")
    return                    
    
async def holding_positions(trade_start_time=None):
    """ """
    backoff = 1
    print("*** STARTING HOLDING POSITIONS PHASE! ***")
    
    # Get the latest trades and trading fees from the start of the "entering positions" phase
    df_spot_trades = get_trades(pair=SPOT_PAIR)
    df_fut_trades = get_trades(pair=FUT_PAIR)
    start_time =  pd.to_datetime('2025-03-17 00:00:00').tz_localize('UTC')
    
    curr_spot_trades = df_spot_trades[(df_spot_trades['tradedAt'] > start_time) & (df_spot_trades['side'] == 'buy')].copy()
    curr_fut_trades = df_fut_trades[(df_fut_trades['tradedAt'] > start_time) & (df_fut_trades['side'] == 'sell')].copy()
    
    curr_spot_trades['bid'] = curr_spot_trades['price']
    curr_spot_trades['ask'] = curr_spot_trades['price']
    curr_spot_trades['pnl'] = curr_spot_trades['quantity'] * (curr_spot_trades['bid']  - curr_spot_trades['price'])
    curr_fut_trades['bid'] = curr_fut_trades['price']
    curr_fut_trades['ask'] = curr_fut_trades['price']
    curr_fut_trades['pnl'] = curr_fut_trades['quantity'] * (curr_fut_trades['price'] - curr_fut_trades['ask'])
    
    spot_pnl = curr_spot_trades['pnl'].sum()
    futures_pnl = curr_fut_trades['pnl'].sum()
    
    fees = curr_fut_trades['fee'].sum() + (curr_spot_trades['fee'] * curr_spot_trades['price']).sum()
    
    df_funding = get_futures_funding(pair=FUT_PAIR)
    df_funding = df_funding[df_funding['createdAt'] > start_time]
    
    funding = df_funding['fundingAmount'].sum()
    
    # start_entering_dt
    # end_entering_dt
    
    # Every hour check the accumulated funding amount
    
    async def update_pnl(msg_raw):
        nonlocal spot_pnl
        nonlocal futures_pnl
        msg = json.loads(msg_raw)
        
        if msg['type'] == 'MARKET_SUMMARY_UPDATE':
            data = msg['data']
            
            if data['currencyPairSymbol'] == SPOT_PAIR:
                curr_spot_trades['bid'] = float( data['bidPrice'])
                curr_spot_trades['ask'] = float( data['askPrice'])
                curr_spot_trades['pnl'] = curr_spot_trades['quantity'] * (curr_spot_trades['bid']  - curr_spot_trades['price'])
                spot_pnl = curr_spot_trades['pnl'].sum()
                
            if data['currencyPairSymbol'] == FUT_PAIR:
                curr_fut_trades['bid'] = float( data['bidPrice'])
                curr_fut_trades['ask'] = float( data['askPrice'])
                curr_fut_trades['pnl'] = curr_fut_trades['quantity'] * (curr_fut_trades['price'] - curr_fut_trades['ask'])
                futures_pnl = curr_fut_trades['pnl'].sum()
        
        # TODO: Print in the same place.
        # 
        print(f'Spot PnL: {round(spot_pnl,4)}')
        print(f'Futures PnL: {round(futures_pnl,4)}')
        print(f'Total PnL: {round(spot_pnl + futures_pnl,4)}')
        print(f'Fees: {round(fees,4)}')
        print(f'Funding: {round(funding,4)}')
        print(f'Net PnL: {round(spot_pnl + futures_pnl - fees + funding,4)}')
        print('-'*55)
    
    # TASK1
    # Stream market summary and update trade PnL
    url = "wss://api.valr.com/ws/trade"
    counter = 0
    while counter < 100:
        try:
            async with websockets.connect(url) as websocket:
                print("[VALR] Connected to WebSocket.")
                
                # Send subscription message
                ms_sub_mesg = {
                    "type": "SUBSCRIBE", 
                    "subscriptions": [{"event": "MARKET_SUMMARY_UPDATE", "pairs": [SPOT_PAIR, FUT_PAIR]}]
                }
                
                await websocket.send(json.dumps(ms_sub_mesg))
                print("[VALR] Subscribed!")
                
                async def send_ping():
                    """ Periodically sends PING messages to keep the connection alive."""
                    while True:
                        await asyncio.sleep(30) # Send PING every 30 seconds
                        await websocket.send(json.dumps({'type':'PING'}))
                        # print("📡 Sent PING to WebSocket")
                        
                # Run PING sender as a background task
                asyncio.create_task(send_ping()) 
                # Note: since send_ping() is tied to the first loop, the task does not persist after the loop exits.

                while counter < 100:
                    msg_raw = await websocket.recv()
                    await update_pnl(msg_raw)
                    counter += 1

        except websockets.ConnectionClosedError:
            print("[VALR] Connection closed. Reconnecting in 5 seconds...")   
            await asyncio.sleep(backoff)
        except Exception as e:
            print(f"[VALR] An error occurred: {e}")
            await asyncio.sleep(backoff)
        
async def exiting_positions():
    """ Handles exiting the spot BTC position and the corresponding futures hedge position. """
    global btc_total_bal
    global btcperp_total_bal
    global trade_qty
    global OpenOrders
    
    print("*** STARTING EXITING POSITIONS! ***")
    balances = [bal for bal in valrClient.get_balances() if float(bal['total']) > 0]
    for bal in balances:
        print(f'{bal["currency"]}: {bal["total"]} | Value: {bal["totalInReference"]}')
    
    btc_total_bal = float([i for i in valrClient.get_balances() if i['currency'] == BASE][0]['total'])
    btcperp_total_bal = float(get_open_futures_positions()['quantity'][0])

    # # Place the first limit-order to sell BTC
    # bid , ask = get_bid_ask()
    # mid_price = round((bid + ask) / 2,0) # Experiment with different prices to set here!
    # limit_order = place_limit_order(pair=SPOT_PAIR, side='sell', price=mid_price, size=trade_qty)
    # OpenOrders.append(limit_order)
    # print('First Limit Order Placed!') # NEEDS FIX check that order is indeed placed.
    
    async def handle_trade_message(msg_raw):
        global btc_total_bal
        global trade_qty
        global OpenOrders
        
        global spot_bid
        global spot_ask
        global fut_bid
        global fut_ask
        global spread
        
        msg = json.loads(msg_raw)
        
        if msg['type'] == 'MARKET_SUMMARY_UPDATE':
            
            if msg.get('currencyPairSymbol','') == SPOT_PAIR:
                spot_bid = float(msg['data']['bidPrice'])
                spot_ask = float(msg['data']['askPrice'])               
            elif msg.get('currencyPairSymbol','') == FUT_PAIR:
                fut_bid = float(msg['data']['bidPrice'])
                fut_ask = float(msg['data']['askPrice'])
                
            try:
                spread = (fut_bid / spot_bid) - 1
                spread_history.append(spread)
                print(f'Spread: {round(spread*100,4)}%')
            except NameError as e:
                print(e)
                
            if len(spread_history) >= 100:
                spread_array = np.array(spread_history)
                spread_mean = spread_array.mean()
                spread_std = spread_array.std()
                
                # print(f'Spread: {spread:.4%} | Mean: {spread_mean:.4%} | Std: {spread_std:.4%}')
                
                if spread < spread_mean - 1.5 * spread_std and btc_total_bal >= trade_qty and len(OpenOrders) == 0: #TODO: Add condition to trade depending on spread
                    # Calculate mid-price
                    mid_price = math.ceil((spot_bid + spot_ask)/2)
                    
                    print(f'Ask Price: {spot_ask}, Bid Price: {spot_bid}, Mid-Price: {mid_price}')
                    
                    limit_order = place_limit_order(pair=SPOT_PAIR, side='sell', price=mid_price, size=trade_qty)
                    OpenOrders.append(limit_order)
            else:
                try:
                    print(f'Spread: {spread:.4%} (collecting data...)')
                except Exception as e:
                    pass
                    
            #TODO:  Update order if it is stale
            # CurrOrderAsk = placed ask (updated in account message handler)
            # if CurrOrderAsk > spot_ask * (1.005) then cancel order
            try:
                for order in OpenOrders:
                    if float(order['price']) >= spot_ask * (1.001):
                        del_order = valrClient.delete_order(SPOT_PAIR, order_id=order['orderId'])
                        print(f'Cancelled stale order: {order["orderId"]}')
            except Exception as e:
                print(f"Failed to delete order. Error: {e}")
                    
    async def stream_trade(url='wss://api.valr.com/ws/trade'):
        """ Stream the trade websocket and handle the market summary updates """
        global btc_total_bal
        global btcperp_total_bal
        global trade_qty
        
        backoff = 1  # Initial backoff time for reconnection
        
        while btc_total_bal >= trade_qty or btcperp_total_bal > 0:
            try:
                async with websockets.connect(url) as websocket:
                    print("[TRADE] Connected to Trade WebSocket.")
                    
                    # Send subscription message
                    ms_sub_mesg = {
                        "type": "SUBSCRIBE", 
                        "subscriptions": [{"event": "MARKET_SUMMARY_UPDATE", "pairs": [SPOT_PAIR, FUT_PAIR]}]
                    }
                    await websocket.send(json.dumps(ms_sub_mesg))
                    print("[TRADE] Subscribed!")
                    
                    async def send_ping():
                        """ Periodically sends PING messages to keep the connection alive."""
                        while True:
                            await asyncio.sleep(40) # Send PING every 30 seconds
                            await websocket.send(json.dumps({"type":"PING"}))
                            # print("📡 Sent PING to WebSocket")
                            
                    # Run PING sender as a background task
                    #asyncio.create_task(send_ping()) 
                    # Note: since send_ping() is tied to the first loop, the task does not persist after the loop exits.

                    while btc_total_bal >= trade_qty or btcperp_total_bal > 0:
                        msg_raw = await websocket.recv()
                        await handle_trade_message(msg_raw)
                        # await asyncio.sleep(3)

            except websockets.ConnectionClosedError:
                print("[TRADE] Connection closed. Reconnecting in 5 seconds...")   
                await asyncio.sleep(backoff)
            except Exception as e:
                print(f"[TRADE] An error occurred: {e}")
                await asyncio.sleep(backoff)
                
    async def handle_account_message(msg_raw):
        global btc_total_bal
        global btcperp_total_bal
        global trade_qty
        global OpenOrders
        
        msg = json.loads(msg_raw)
        print(msg['type'])
    
        # Print if limit order has been placed successfully
        
        if msg['type'] == 'OPEN_ORDERS_UPDATE':
            OpenOrders = msg['data']
            print(msg['data'])
        
        # Handle BALANCE_UPDATE (update btc_avail_bal)
        if msg['type'] == 'BALANCE_UPDATE':
            data = msg['data']
            
            if data['currency']['symbol'] == BASE:
                btc_total_bal = float(data['total']) # Update the BTC Total Balance
                print(f'{data["updatedAt"]} | {BASE} Available Balance: {btc_total_bal}')
        
        # If limit buy order is filled --> Place long futures position @ market
        if msg['type'] == 'ORDER_STATUS_UPDATE': 
            data = msg.get('data',{})
            print(f'{data["orderUpdatedAt"]} | Order {data["orderStatusType"]}: buy {data["originalQuantity"]} {data["currencyPair"]} at {data["executedPrice"]}.')
            
            if data.get('orderStatusType','') == 'Failed':
                OpenOrders = []
            
            if data.get('orderStatusType','') == 'Filled' and data.get('currencyPair','') == SPOT_PAIR and data.get('orderSide','') == 'sell':
                # Place long futures position of similar size (i.e. close short position)
                market_order = post_market_order(pair=FUT_PAIR, side='buy', size=data['originalQuantity'])
        
            # Place another limit sell order(spot) if there are no .
            
        if msg['type'] == 'POSITION_CLOSED ':
            data = msg.get('data',{})
            btcperp_total_bal = 0 ## TODO! Make this operation more robust.
            print("Futures position update to: {btcperp_total_bal}")
            
        if msg['type'] == 'NEW_ACCOUNT_TRADE ':
            data = msg.get('data',{})
            dt = pd.to_datetime(data.get('tradedAt')) # _{dt.strftime("%Y%m%d")}
            async with aiofiles.open(f'data/valr/funding_arb_trades.txt', mode='a') as f:
                await f.write(json.dumps(data) + "\n")
            
    async def stream_account():
        global btc_total_bal
        global trade_qty
        global btcperp_total_bal
        
        # Open WebSocket Connection
        while btc_total_bal >= trade_qty or btcperp_total_bal > 0:
            try:
                async with websockets.connect(WSS_URL, extra_headers=get_valr_headers(API_KEY, API_SECRET)) as ws:
                #async with websockets.connect(WSS_URL, additional_headers=get_valr_headers(API_KEY, API_SECRET)) as ws:
                    print('[ACCOUNT] Websocket Connection Established!')
                    while btc_total_bal >= trade_qty or btcperp_total_bal > 0:
                        msg_raw = await ws.recv() # Waits for WebSocket message (blocking)
                        await handle_account_message(msg_raw)
                        
            except Exception as e:
                # Handle Errors & Re-connect
                print(f"Error: {e}") 
                    
    # Unsubscribe and Close websocket connection
    
    # Hand over to the start of the main procedure
    
    await asyncio.gather(stream_trade(), stream_account())
    
    # If all BTC is sold, return control to `main()`
    print("*** EXITING POSITIONS COMPLETE! RESTARTING STRATEGY ***")
    return
    
#%% MAIN    
def main():
    """ Manages the lifecycle of the trading strategy """
    print("*** STARTING ARBITRAGE STRATEGY ***")
    
    pass
    # Initialize the program
    # Get starting account value/ values
    # and start time, etc.
    
    #task1() # entering_positions
    # print("STARTING TO ENTER POSITIONS")
    
    # print("POSITION ENTERING COMPLETE")
    
    #task2() # holding_positions / updating position status
    # print("STARTING HOLDING PHASE")
    
    # print("HOLDING PHASE DONE")
    
    #task3() # exiting positions
    # print("STARTING TO EXIT POSITIONS)
    
if __name__ == '__main__':
    try:
        asyncio.run(entering_positions())
    except KeyboardInterrupt:
        print("Shutting down WebSocket connection gracefully.")
        print(f'Initial {QUOTE} Balance: {usdt_initial_bal}')
        # Initial Available USDT
        # Initial Total USDT
        # Initial Reserved USDT 
        

#%% EXPERIMENT

if False:
    df1 = get_trades(pair='BTCUSDT')
    df2 = get_trades(pair='BTCUSDTPERP')
    start_time =  pd.to_datetime('2025-03-12 21:00:00').tz_localize('UTC')
    
    df = pd.concat([df1,df2])
    curr_trades = df[df['tradedAt'] > start_time]
    
    curr_spot_trades = df1[df1['tradedAt'] > start_time]
    curr_fut_trades = df2[df1['tradedAt'] > start_time]
    
    spot_pnl = 0
    futures_pnl = 0
    with open('data/valr/MARKET_SUMMARY_UPDATE.txt', mode='r') as file:
        for line in file:
            data = json.loads(line).get('data')
            
            if data['currencyPairSymbol'] == 'BTCUSDT':
                curr_spot_trades['bid'] = float( data['bidPrice'])
                curr_spot_trades['ask'] = float( data['askPrice'])
                curr_spot_trades['pnl'] = curr_spot_trades['quantity'] * (curr_spot_trades['bid']  - curr_spot_trades['price'])
                spot_pnl = curr_spot_trades['pnl'].sum()
                
            if data['currencyPairSymbol'] == 'BTCUSDTPERP':
                curr_fut_trades['bid'] = float( data['bidPrice'])
                curr_fut_trades['ask'] = float( data['askPrice'])
                curr_fut_trades['pnl'] = curr_fut_trades['quantity'] * (curr_fut_trades['price'] - curr_fut_trades['ask'])
                futures_pnl = curr_fut_trades['pnl'].sum()
                
            print(f'Spot PnL: {round(spot_pnl,4)}')
            print(f'Futures PnL: {round(futures_pnl,4)}')
            print(f'Total PnL: {round(spot_pnl + futures_pnl,4)}')
                
            #time.sleep(1)

if False:
    start_time =  pd.to_datetime('2025-03-12 21:00:00').tz_localize('UTC')
    df_funding = get_futures_funding(pair='BTCUSDTPERP')
    df_funding = df_funding[df_funding['createdAt'] > start_time]
    
    print(round(df_funding['fundingAmount'].sum(),4))
    print(round(df_funding['fundingRate'].sum(),4))
    
if False:
    bid = get_bid_ask()[0]
    ask = get_bid_ask()[1]
    order = place_limit_order(pair='BTCUSDT', side='buy', price=bid, size=0.0003)
    
    # Sell off some BTC
    ask = get_bid_ask()[1]
    order = place_limit_order(pair='BTCUSDT', side='sell', price=ask, size=0.0003)
    
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
    
    
    
    
    
        
    
    
    
    
    

    
    
