# Kucoin Launch Bot

#%% Libraries
import sys
import os
import pandas as pd
import numpy as np
import math
import asyncio
import aiofiles
import socket
import json
import time
from datetime import datetime
from configparser import ConfigParser
from kucoin.client import WsToken
from kucoin.ws_client import KucoinWsClient
from kucoin.client import Market
from kucoin.client import Trade
from kucoin.client import User

curr_dir = os.getcwd()
sys.path.append(curr_dir)
from scripts import my_kucoin

#%% Utilities
def current_time():
    # Get the current time
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#%% Config
config = ConfigParser()
config.read('algo_trading.cfg')
api_key = config.get('kucoin', 'api_key')
api_secret = config.get('kucoin', 'api_secret')
api_passphrase = config.get('kucoin', 'api_passphrase')

#%% Connect to API
marketClient = Market(url='https://api.kucoin.com')
tradeClient = Trade(key=api_key, secret=api_secret, passphrase=api_passphrase, is_sandbox=False, url='')
userClient = User(key=api_key, secret=api_secret, passphrase=api_passphrase, is_sandbox=False)

#%% Inputs / Parameters

if len(sys.argv) != 2:
    print("ERROR - Usage: python script.py <symbol>")
    sys.exit(1)

# TODO: Make the below inputs required when the script is run in command prompt or using bash.
SYMBOL = sys.argv[1]
AMOUNT = 9 # amount of USDT to trade
TAKE_PROFIT = 0.1 # Take profit after a 50% price change
STOP_LOSS = 0.5

#%%

async def main(symbol):
    
    print(f'{current_time()} | INFO | Start process...')
    print(f'   Symbol: {SYMBOL}')
    print(f'   Amount: {AMOUNT}')
    print(f'   Take Profit: {TAKE_PROFIT}')
    print(f'   Stop Loss: {STOP_LOSS}')
    
    EXECUTED = True
    HOLDING = True # available
    
    async def deal_msg(msg):
            nonlocal EXECUTED
            nonlocal HOLDING
                        
            if not EXECUTED:
                print(f'{current_time()} | INFO | No trades executed.')
                
                # Get besk ask price
                try:
                    ask_price =  my_kucoin.get_ask(symbol)
                    print(f'{current_time()} | INFO | Ask Price: {ask_price}')
                except Exception as e:
                    print(f'ERROR: {e}')
                
                # Get Instrument Information
                res = marketClient.get_symbol_list_v2(symbol=symbol)
                result = [d for d in res if d.get('symbol') == symbol][0]
                p = float(result.get('priceIncrement'))     # price precision
                q = float(result.get('baseIncrement'))      # quantity precision
                q_min = float(result.get('baseMinSize'))    # minimum quantity
                
                time.sleep(1) # Sleep for one second, to manage API rate limits.
                
                #*********************************************************************************************************
                # Check Existing Balance
                orders = tradeClient.get_fill_list(tradeType='TRADE', symbol=symbol).get('items')

                if len(orders) > 0:
                    orders_df = pd.DataFrame(orders)
                    buys_df = orders_df[orders_df['side'] == 'buy']
                    size = float(buys_df['size'].apply(pd.to_numeric).sum())
                    price = float(buys_df['price'].apply(pd.to_numeric).mean())
                    stop_price = price * (1+0.1) # 10% profit Limit Order #TODO:
                    stop_price = round(round(stop_price / p) * p, int(abs(math.log10(p))))
                    print(f'{current_time()} | INFO |  Existing position of {size}, bought at {price}.')
                    
                    if q==1:
                        size=int(size)
                    
                    # Set Exit Price for Current Position
                    try:
                        limit_order = tradeClient.create_limit_order(SYMBOL, 'sell', str(size), str(stop_price))
                        print(f'{current_time()} | INFO | Limit order set for existing position.')
                    except Exception as e:
                        print(f'{current_time()} | INFO | Failed to place limit order for existing position')
                        print(f'ERROR: {e}')
                else:
                    print(f'{current_time()} | INFO | No existing position (No orders filled.)')
                ##TODO: Need an else statement here. Either we exit an exist pre-market order or we enter into one.
                ##TODO: Also if we are entering into one, we need to cancel any non filled pre-market orders.
                
                #********************************************************************************************************
                
                # Create Buy Order (if no buy order has been created)
                qty = AMOUNT / float(ask_price) # need to round down
                qty = round(math.floor(qty / q) * q, int(abs(math.log10(q))))
                qty = int(qty) if q == 1 else qty
                
                # Place buy order
                try:
                    order_id = tradeClient.create_limit_order(SYMBOL, 'buy', str(qty), ask_price)
                    print(f'{current_time()} | INFO | Buy limit order placed for {qty} {SYMBOL} at {ask_price}.')
                except Exception as e:
                    print(f'{current_time()} | INFO | Failed to place buy limit order!')
                    print(f'ERROR: {e}')
                    
                
                # Place take profit
                take_profit = float(ask_price) * (1+TAKE_PROFIT)
                take_profit = round(round(take_profit / p) * p, int(abs(math.log10(p))))
                try:
                    tp_order = tradeClient.create_limit_order(SYMBOL,
                                                              side='sell',
                                                              size=str(qty),
                                                              price=str(take_profit)
                                                              )
                    print(f'{current_time()} | INFO | Take profit set at {take_profit}.')
                except Exception as e:
                    print(f'{current_time()} | INFO | Failed to place take profit limit stop order!')
                    print(f'ERROR: {e}')

                # Place stop loss
                stop_loss = float(ask_price) * (1-STOP_LOSS)
                stop_loss = round(round(stop_loss / p) * p, int(abs(math.log10(p))))
                try:
                    sl_order = tradeClient.create_limit_stop_order(SYMBOL,
                                                                    side='sell',
                                                                    size=str(qty),
                                                                    price=str(stop_loss),
                                                                    stopPrice=str(stop_loss))
                    print(f'{current_time()} | INFO | Stop loss set at {stop_loss}.')
                except Exception as e:
                    print(f'{current_time()} | INFO | Failed to place stop loss limit stop order!')
                    print(f'ERROR: {e}')
                
                EXECUTED = True
            
            if HOLDING == True:
                
                # Get balance amount
                balance = 90 #userClient.get_account_list()
                
                # Set take profit (0.2)
                take_profit = 0.2
                #take_profit = round(round(take_profit / p) * p, int(abs(math.log10(p))))
                qty = balance
                try:
                    tp_order = tradeClient.create_limit_order(SYMBOL,
                                                              side='sell',
                                                              size=str(qty),
                                                              price=str(take_profit)
                                                              )
                    print(f'{current_time()} | INFO | Take profit set at {take_profit}.')
                    HOLDING = False
                except Exception as e:
                    print(f'{current_time()} | INFO | Failed to place take profit limit stop order!')
                    print(f'ERROR: {e}')
                    
            # After a minute has lapsed, set stop loss
                # how do we determine a minute has lapsed.
                
                
            ## Manage Trade
            # Ideas:
            # If the sell velocity is increasing, exit the 
            
            
            ## SAVE DATA!
            today = datetime.now().date()
            if msg['topic'] == f'/market/match:{symbol}':
                data = msg['data']
                data_str = json.dumps(data) # converts a subset of python objects into a json string.
                async with aiofiles.open(f"{symbol} - trades - {today}.txt", mode = "a") as f:
                    await f.write(data_str + "\n") # await just tells python: "while doing this job is running, if there is nothing to do, feel free to go an do something else."
            
            elif msg['topic'] == f'/spotMarket/level2Depth5:{symbol}':
                data = msg['data']
                data_str = json.dumps(data)
                async with aiofiles.open(f"{symbol} - OB spanshots - {today}.txt", mode = "a") as f:
                    await f.write(data_str + "\n")
                    
    # is public
    client = WsToken()

    ws_client = await KucoinWsClient.create(None, client, deal_msg, private=False)

    await ws_client.subscribe(f'/market/match:{symbol}')
    await ws_client.subscribe(f'/spotMarket/level2Depth5:{symbol}')
    
    #TODO: subscribe to the trading activity topic.
    
    while True:
        await asyncio.sleep(60)
        
#%%
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <symbol>")
        sys.exit(1)
    
    symbol = sys.argv[1]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol))
