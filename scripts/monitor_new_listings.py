# Monitor New Listings

#%% Libraries
import pandas as pd
import numpy as np
import time
import requests
from configparser import ConfigParser

from datetime import datetime

from kucoin.client import Market
from pybit.unified_trading import HTTP
from binance.client import Client

#%% Config

config = ConfigParser()
config.read('algo_trading.cfg')
pushover_api_token = config.get('pushover', 'api_token')
pushover_user_key = config.get('pushover', 'user_key')


#%% Utilities
def current_time():
    # Get the current time
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def send_pushover_notification(new_symbols, exchange):

    message = f"New symbols added on {exchange}: {', '.join(new_symbols)}"
    
    payload = {
        'token':pushover_api_token,
        'user':pushover_user_key,
        'device':'SS3',
        'message':message,
        'title':f'{exchange} New Listing Alert'
        }
    
    try: 
        response = requests.post('https://api.pushover.net/1/messages.json', data=payload)
        if response.status_code == 200:
            print(f"Pushover notifiction sent: {message}")
        else:
            print(f"Failed to send Pushover notification: {response.text}")
    except Exception as e:
        print(f"Error sending Pushover notification: {e}")

#%% Connect to API
marketClient = Market(url='https://api.kucoin.com')
session = HTTP(testnet=False)
binance_client = Client()

#%% Main

# Initial Symbol List
res = marketClient.get_symbol_list_v2() # Kucoin
bybit_res = session.get_instruments_info(category="spot")['result']['list'] # Bybit
binance_res = binance_client.get_exchange_info()['symbols'] # Binance

existing_symbols = [x['symbol'] for x in res]
bybit_existing_symbols = [x['symbol'] for x in bybit_res]
binance_existing_symbols = [x['symbol'] for x in binance_res]

# Remove the last 2 for testing
existing_symbols = existing_symbols[:-2]

print(f"{current_time()} | INFO | Starting Process")
print(f"Initial State:")
print(f"KuCoin Number of symbols: {len(existing_symbols)}")
print(f"Bybit Number of symbols: {len(bybit_existing_symbols)}")
print(f"Binance Number of symbols: {len(binance_existing_symbols)}")
print("-"*55)

# Every 5 minutes check if there has been a new symbol added
while True:
    res = marketClient.get_symbol_list_v2()
    bybit_res = session.get_instruments_info(category="spot")['result']['list']
    binance_res = binance_client.get_exchange_info()['symbols']
    
    latest_symbols = [x['symbol'] for x in res]
    bybit_latest_symbols = [x['symbol'] for x in bybit_res]
    binance_latest_symbols = [x['symbol'] for x in binance_res]
    ## TODO: Create a function that returns the list of symbols and exchange name. 
    ##       - This function will be run concurrently in mutiple threads for each exchange.
    
    # Check for any symbols in latest_symbols not in existing_symbols
    new_symbols = list(set(latest_symbols) - set(existing_symbols))
    bybit_new_symbols = list(set(bybit_latest_symbols) - set(bybit_existing_symbols))
    binance_new_symbols = list(set(binance_latest_symbols) - set(binance_existing_symbols))
    
    if new_symbols:
        print(f"{current_time()} | INFO | New symbols added on Kucoin: {new_symbols}")
        send_pushover_notification(new_symbols, exchange="KuCoin")
        existing_symbols.extend(new_symbols)
        
    if bybit_new_symbols:
        print(f"{current_time()} | INFO | New symbols added on Bybit: {bybit_new_symbols}")
        send_pushover_notification(bybit_new_symbols, exchange="Bybit")
        bybit_existing_symbols.extend(bybit_new_symbols)
        
    if binance_new_symbols:
        print(f"{current_time()} | INFO | New symbols added on Binance: {binance_new_symbols}")
        send_pushover_notification(binance_new_symbols, exchange="Binance")
        binance_existing_symbols.extend(binance_new_symbols)
        
    # Wait for 5 minutes
    time.sleep(300)
    
    
# 2020-05-30 00:00:00 | ALERT | New  Listing on Bybit: ZKL-USDT
#       Symbol: ZKL-USDT
#       Trade Enanbled: False
#       Price Precision: 0.01
#       Quantity Precision: 0.01
#       Minimum Quantity: 0.01
# ---------------------------------------------------------------