#**********************************
# Stream Futures - Spot Spread
#**********************************

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

#%% INPUTS
WSS_URL = 'wss://api.valr.com/ws/trade'
PAIR = 'BTCZAR'

#%% FUNCTION
async def handle_message(msg_raw):
    global spot_bid
    global spot_ask
    global fut_bid
    global fut_ask
    global spread
    global sec_key
    global latest_values
    global last_spread
    global avg_spread
    
    msg = json.loads(msg_raw)
    
    if msg['type'] == 'MARKET_SUMMARY_UPDATE':
        
        dt = pd.to_datetime(msg['data']['created'])
        pair = msg['currencyPairSymbol']
        
        if pair == PAIR:
            spot_bid = bid = float(msg['data']['bidPrice'])
            spot_ask = ask = float(msg['data']['askPrice'])
        elif pair == PAIR+"PERP":
            fut_bid = bid = float(msg['data']['bidPrice'])
            fut_ask = ask = float(msg['data']['askPrice'])
        try:
            spread = fut_bid / spot_bid - 1
            # print spread only when it changes.
            print(f'{dt} | Spread: {round(spread*100,4)}%  Avg: {round(avg_spread*100,4)}')
            
            t = dt.floor('S')
            
            if sec_key is None:
                sec_key = t
            
            if sec_key != t:
                latest_values.append(last_spread)
                sec_key = t
            
            last_spread = spread
            
            avg_spread = np.mean(latest_values)
            
            
            
            data = {'datetime':str(dt), 'pair':pair, 'bid':bid, 'ask':ask}
            async with aiofiles.open(f'data/valr/{PAIR.lower()}_fut_spot_spread_{dt.strftime("%Y%m%d")}.txt', mode='a') as f:
                await f.write(json.dumps(data) + "\n")
        
        except Exception as e:
            print(f'Error: {e}')
            

            
async def stream_trade(url='wss://api.valr.com/ws/trade'):
    """ Stream the trade websocket and handle the market summary updates """
    backoff = 1  # Initial backoff time for reconnection
    
    while True:
        try:
            async with websockets.connect(url) as websocket:
                print("[TRADE] Connected to Trade WebSocket.")
                
                # Send subscription message
                ms_sub_mesg = {
                    "type": "SUBSCRIBE", 
                    "subscriptions": [{"event": "MARKET_SUMMARY_UPDATE", "pairs": [PAIR, PAIR+"PERP"]}]
                }
                await websocket.send(json.dumps(ms_sub_mesg))
                print("[TRADE] Subscribed!")
                
                async def send_ping():
                    """ Periodically sends PING messages to keep the connection alive."""
                    while True:
                        await asyncio.sleep(30) # Send PING every 30 seconds
                        await websocket.send(json.dumps({"type":"PING"}))
                        # print("📡 Sent PING to WebSocket")
                        
                # Run PING sender as a background task
                #asyncio.create_task(send_ping()) 
                # Note: since send_ping() is tied to the first loop, the task does not persist after the loop exits.

                while True:
                    msg_raw = await websocket.recv()
                    await handle_message(msg_raw)

        except websockets.ConnectionClosedError:
            print("[TRADE] Connection closed. Reconnecting in 5 seconds...")   
            await asyncio.sleep(backoff)
        except Exception as e:
            print(f"[TRADE] An error occurred: {e}")
            await asyncio.sleep(backoff)
            
#%% MAIN

if __name__ == '__main__':
    latest_values = deque(maxlen=86400)
    sec_key = None
    last_spread = None
    avg_spread = 0
    
    try:
        asyncio.run(stream_trade())
    except KeyboardInterrupt:
        print("Shutting down WebSocket connection gracefully.")

