# Monitor New Listings

#%% Libraries
import pandas as pd
import numpy as np
import time

from datetime import datetime

from kucoin.client import Market
#%% Utilities
def current_time():
    # Get the current time
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#%% Connect to API
marketClient = Market(url='https://api.kucoin.com')

#%% Main

# Initial Symbol List
res = marketClient.get_symbol_list_v2()

existing_symbols = [x['symbol'] for x in res]

# Remove the last 2 for testing
#existing_symbols = existing_symbols[:-2]

print(f"{current_time()} | INFO | Starting Process")
print(f"Initial State: Count = {len(existing_symbols)} symbols")

# Every 5 minutes check if there has been a new symbol added
while True:
    res = marketClient.get_symbol_list_v2()
    
    latest_symbols = [x['symbol'] for x in res]
    
    # Check for any symbols in latest_symbols not in existing_symbols
    new_symbols = list(set(latest_symbols) - set(existing_symbols))
    
    if new_symbols:
        print(f"{current_time()} | INFO | New symbols added: {new_symbols}")
        existing_symbols.extend(new_symbols)
        
    # Wait for 5 minutes
    time.sleep(300)
    
    
# 2020-05-30 00:00:00 | ALERT | New  Listing on Bybit: ZKL-USDT
#       Symbol: ZKL-USDT
#       Trade Enanbled: False
#       Price Precision: 0.01
#       Quantity Precision: 0.01
#       Minimum Quantity: 0.01
# ---------------------------------------------------------------