#***************************************************
# VALR Cross Sectional Momentum Strategy
#***************************************************


"""
 - (2) Add more functionality for leverage management
 - Include account monitor in the script logging info to show PnL / Total Value etc.
 - Run code only on new bar. 
"""

import sys
import os
import math
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

#%% Libraries
import pandas as pd
import numpy as np
import json
import time
import hashlib
import hmac
import requests
import threading
from datetime import datetime, timedelta
from configparser import ConfigParser
from valr_python import Client
from valr_python.exceptions import IncompleteOrderWarning, RESTAPIException
from decimal import Decimal
from scripts.data_download import get_bybit_data # We have to get the data from bybit

#%% Configurations
conf_file = 'algo_trading.cfg'
config = ConfigParser()
config.read(conf_file)

API_KEY = config.get('valr2', 'api_key')
API_SECRET = config.get('valr2', 'api_secret')

#%% Connect to APIs
valrClient = Client(api_key=API_KEY, 
                    api_secret=API_SECRET)

#%% Inputs
LOOKBACK = 24*7 # The number of hours to calculate momentum lookback over
REBAL_FREQ = '6H'
UNIVERSE = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'DOGEUSDT', 'SOLUSDT', 'AVAXUSDT']
UNIVERSE_PERP = [pair + "PERP" for pair in UNIVERSE]
LEVERAGE = 2

#%% Functions
def apply_qty_filters(raw_qty, qty_min, qty_dcp):
    """
    Rounds down raw_qty to a specified decimal place and 
    applies a minimum quantity filter.
    """
    # First round down
    qty = math.floor((raw_qty / 10**(-qty_dcp))) * 10**(-qty_dcp)
    
    # Apply minimum quantity filter
    if abs(qty) < qty_min:
        qty = 0
        
    return qty
    
def fetch_symbol_data(pair, start_time, results):
    """Fetches Bybit data for a single symbol and stores it in results dict."""
    try:
        df = get_bybit_data('linear', symbol=pair, interval=60, start_time=start_time, verbose=False)
        df.set_index('Time', inplace=True)
        results[pair] = df[['Close']].rename(columns={'Close': pair})
    except Exception as e:
        print(f"Error fetching data for {pair}: {e}")

def get_latest_data():
    """Fetches latest market data concurrently using threading."""
    now = datetime.utcnow()
    start_time = (now - timedelta(hours=LOOKBACK + 2)).strftime('%Y-%m-%d %H:%M:%S')

    threads = []
    results = {}

    for pair in UNIVERSE:
        thread = threading.Thread(target=fetch_symbol_data, args=(pair, start_time, results))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()  # Wait for all threads to finish

    df_close = pd.concat(results.values(), axis=1)
    
    df_close = df_close[UNIVERSE]
    
    return df_close

def get_signals(data):
    """Generates the target weights given the latest data"""
    
    df_mom = data.pct_change(LOOKBACK)
    
    mom_rank = df_mom.iloc[[-2]].rank(axis=1, ascending=False)
    
    target_wghts = mom_rank.applymap(lambda x: 1/3 if x < 4 else -1/3).to_dict('records')[0]
    
    return target_wghts

def get_portfolio_value():
    total_value = float(valrClient.get_balances()[0]['total'])
    return total_value

def generate_target_portfolio(target_wghts):
    """ Generate the quantity per pair based on the target weight vector"""
    
    # get current total portfolio value
    total_value = get_portfolio_value()
    
    # Need bid and ask prices
    market = valrClient.get_market_summary()
    prices = {q['currencyPair'].replace("PERP",""):float(q['bidPrice']) for q in market if q['currencyPair'] in UNIVERSE_PERP}
    
    target_exp = {p: total_value * w * LEVERAGE for p,w in target_wghts.items()}
    
    target_qty = {p: target_exp[p] / prices[p] for p in target_exp if p in prices}
    
    # round the targer_qty values in accordance with the price filters
    qty_filters_min = {'BTCUSDT':0.0001, 'ETHUSDT':0.001, 'SOLUSDT':0.01, 'DOGEUSDT':6, 'XRPUSDT':2, 'AVAXUSDT':0.03} # minimum base amount
    dty_filters_dp = {'BTCUSDT':4, 'ETHUSDT':3, 'SOLUSDT':2, 'DOGEUSDT':0, 'XRPUSDT':0, 'AVAXUSDT':2} # decimal places
    
    target_qty = {p:round(target_qty[p],dty_filters_dp[p]) for p in target_qty}
    
    return target_qty

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
 
def get_valr_headers(api_key, api_secret):
    valr_headers = {}
    
    path = '/v1/positions/open'
    timestamp = int(time.time() * 1000)
    valr_headers["X-VALR-API-KEY"] = api_key
    valr_headers["X-VALR-SIGNATURE"] = sign_request(api_key_secret=api_secret, timestamp=timestamp,
                                                     verb='GET', path=path)
    valr_headers["X-VALR-TIMESTAMP"] = str(timestamp)  # str or byte req for request headers
    
    return valr_headers
        
def get_positions():
    """Gets the current postions in the portfolio"""

    url = "https://api.valr.com/v1/positions/open"
    
    payload={}
    headers = get_valr_headers(API_KEY, API_SECRET)
    
    response = requests.request("GET", url, headers=get_valr_headers(API_KEY, API_SECRET), data=payload)
    
    positions = json.loads(response.text)
    
    curr_positions = {pos['pair'].replace('PERP',''):float(pos['quantity']) * (-1 if pos['side'] == 'sell' else 1) for pos in positions}
    
    return curr_positions

def generate_rebal_orders(curr_positions, target_qty):
    
    rebalance_orders = {pair:(target_qty[pair] - curr_positions.get(pair,0)) for pair in target_qty.keys()}
    #TODO: Will need consider a union of pairs in both target_qty and curr_positions
    # i.e. consistent key formatting in both dictionaries.
    
    # round the targer_qty values in accordance with the price filters
    qty_filters_min = {'BTCUSDT':0.0001, 'ETHUSDT':0.001, 'SOLUSDT':0.01, 'DOGEUSDT':6, 'XRPUSDT':2, 'AVAXUSDT':0.03} # minimum base amount
    dty_filters_dcp = {'BTCUSDT':4, 'ETHUSDT':3, 'SOLUSDT':2, 'DOGEUSDT':0, 'XRPUSDT':0, 'AVAXUSDT':2} # decimal places
    
    rebalance_orders = {pair:apply_qty_filters(rebalance_orders[pair], qty_filters_min[pair], dty_filters_dcp[pair]) for pair in rebalance_orders}
    
    return rebalance_orders

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

if False:
    order = post_market_order(pair='BTCZARPERP', side='BUY', size=0.0002)

def execute_trades(rebalance_orders):
    """ Execute the necessary orders to rebalance the portfolio on the exchange."""
    for pair, qty in rebalance_orders.items():
        if qty > 0:
            post_market_order(pair=pair+'PERP', size=abs(qty), side='buy')
        elif qty < 0:
            post_market_order(pair=pair+'PERP', size=abs(qty), side='sell')
        else:
            # Do nothing
            pass
        
def on_exit():
    pass

#%% Main
def main():
    """ Main execution loop for cross-sectional momentum strategy."""
    print(f'{datetime.now()} | Starting process...')
    print(f'Starting portfolio value: {round(get_portfolio_value(),2)}')
    print('-'*55)
    
    current_bar = None
    # Get initial data
    
    while True:
        now = datetime.utcnow()
        # TODO: Insert Portfolio Updates on the same line, creating a print line only for the print statements below.
        # i.e. new print lines when the program moves on.
        print(current_bar)
        
        # TODO: We need some print statements here, to let us know the status of the program.
        if now.hour % 6 == 0: # Every 6 hours
            
            # Fetch latest market data
            data = get_latest_data()
            
            if current_bar == data.index[-1]:
                time.sleep(2) # Need to sleep for a bit in order to avoid hitting API rate limits.
                continue # The continue keyword is used to end the current iteration in loop.
            else:
                current_bar = data.index[-1]
            
            print(f'{datetime.now()} | Rebalancing ...')
            print('Latest Data:')
            print(data.tail())
            
            # Generate target weights
            target_wghts = get_signals(data)
            print('Target Weights:')
            print(target_wghts)
            
            # Generate target quantity
            target_qty = generate_target_portfolio(target_wghts)
            print('Target Portfolio:')
            print(target_qty)
            
            # Get current positions
            curr_positions = get_positions()
            
            # Generate rebalancing orders
            rebalance_orders = generate_rebal_orders(curr_positions, target_qty)
            print('Rebalancing Orders:')
            print(rebalance_orders)
            
            # """ 
            # ERROR:
            #     KeyError: 'BTCZAR'
            # """
            
            # Execute trades / orders
            execute_trades(rebalance_orders)
            print(f'{datetime.now()} Rebalancing complete.')
            print('-'*55)
            
            
if __name__ == "__main__":
    main()
            
            
            
            
            
            
            
        
            


    





