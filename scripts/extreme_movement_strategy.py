# Extreme Movement Strategy
# This script automates a trading strategy where by a long position is 
# taken for any asset that have moved by more than 5% in the last hour.

# Import Libraries
import pandas as pd
import numpy as np
from pybit.unified_trading import HTTP 
from pybit.unified_trading import WebSocket
from time import sleep
from datetime import datetime, timezone
from configparser import ConfigParser
import math
from decimal import Decimal, ROUND_DOWN
#import pytz

config = ConfigParser()
config.read('algo_trading.cfg')
api_key = config.get('bybit', 'api_key')
api_secret = config.get('bybit', 'api_secret')

client = HTTP(testnet=False,
              api_key=api_key,
              api_secret=api_secret)

# Get USDT balance
def get_available_balance(coin='USDT', accountType='UNIFIED'):
    response = client.get_wallet_balance(accountType=accountType, coin=coin)
    balance = float(response['result']['list'][0]['coin'][0]['walletBalance'])
    return balance

balance = get_available_balance()
print(f'Account USDT Balance: {balance}')

def round_down(value, n):
    rounded_down = math.floor(value/n) * n
    return rounded_down

# Function to download all tickers and determine assets with extreme movements
def get_signals():
    response = client.get_tickers(category='linear')
    result = response['result']
    df = pd.DataFrame(result['list'])
    
    # convert columns to numeric
    df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric)
    
    # calculate hourly percentage change
    df['1hPct'] =  df['lastPrice'] / df['prevPrice1h'] - 1
    
    # Filter for the top 100 based on 24 hour turnover
    df_top100 = df.nlargest(100, 'turnover24h')
    #TODO: Filter for assets that have been in-force for a while.
    #TODO: Add other filtering criteria, e.g. results from ML model

    #NOTE: Should there be a time factor on these trades?
    
    # Sort by the 1 hour percentage change
    df_top100 = df_top100.sort_values(by='1hPct', ascending=False, ignore_index=True)
    
    longs = df_top100[df_top100['1hPct'] > 0.05]
    shorts = df_top100[df_top100['1hPct'] < -0.05].sort_values(by='1hPct', ascending=True, ignore_index=True)
    
    
    signals = []
    
    # long signals
    if len(longs) > 0:
        asset = longs['symbol'][0]
        pct_change = longs['1hPct'][0]
        signal = {'asset':asset, '1hPct':pct_change, 'action':'Long'}
        signals.append(signal)
    
    # short 
    if len(shorts) > 0:
        asset = shorts['symbol'][0]
        pct_change = shorts['1hPct'][0]
        signal = {'asset':asset, '1hPct':pct_change, 'action':'Short'}
        signals.append(signal)
        
    return signals


# Callback function (for websocket stream)
def handle_message(message):
    
    ts = message['data'][0]['timestamp']
    dt = pd.to_datetime(ts, unit='ms')
    #start_dt = pd.Timestamp('1998-12-22 14:30:00', tz=pytz.UTC)
        
    # if close of bar (run analysis)
    if message['data'][0]['confirm']:
        print('')
        print(message)
        
        # Get the trade signal
        #TODO: Improve the trade signal function to include all my trading logic!
        signals = get_signals()
        
        if len(signals) == 0:
            print('\nNo signal!')
            return
            
        # Loop through each action and exectue orders
        for signal in signals:
            print(f'\n{dt} | TRADE SIGNAL: {signal}')
            #TODO: Check the current positions of the assets/symbols
            symbol = signal['asset']

            # Get USDT balance
            response = client.get_wallet_balance(accountType='UNIFIED', coin='USDT')
            balance = float(response['result']['list'][0]['coin'][0]['walletBalance'])
        
            # Get instrument info
            #TODO: Read instrument info into memory and keep there unless an update is needed
            # ...  Request info on start of script, then use the stored data to retrieve the required info. 
            res = client.get_instruments_info(category='linear', symbol=symbol)
            result = res['result']['list'][0]
            tick_size = float(result['priceFilter']['tickSize'])
            precision = int(-math.log10(tick_size))
            qty_step = float(result['lotSizeFilter']['qtyStep'])
            qty_prec = int(-math.log10(qty_step))
        
            # Calculate quantity
            # get best ask price
            res = client.get_tickers(category='linear', symbol=symbol)
            ask_price = float(res['result']['list'][0]['ask1Price'])
            print(f'\nAsking price is: {ask_price}')
            # calculate quantity allowing for the quantity step size of the instrument
            quantity = round_down(balance/ask_price, n=qty_step)
            print(f'\nCalculated quantity: {quantity}')
            
       #TODO: Set leverage
       #TODO: Place trailing stop losses
       #TODO: Add error handling for placing orders (or any other API requests)
       
            if signal['action'] == 'Long':
                
                # Calculate take profit and stop loss
                take_profit = round(ask_price * (1+0.05) / tick_size) * tick_size
                stop_loss = round(ask_price * (1-0.05) / tick_size) * tick_size
                print(f'\nTake Profit={take_profit}, Stop Loss={stop_loss}\n')
                
                #TODO: Include appropriate exception handling for placing trades.
                try:
                    # Long asset
                    buy_order = client.place_order(
                                    category="linear",
                                    symbol=symbol,
                                    side="Buy",
                                    orderType="Market",
                                    qty=f'{quantity:.{qty_prec}f}',              # TODO: Need to calculate | Also need to get coin precision
                                    price="15600",          # Market order will ignore this field
                                    takeProfit=f'{take_profit:.{precision}f}',
                                    stopLoss=f'{stop_loss:.{precision}f}',
                                    timeInForce="PostOnly",
                                    orderLinkId=f"{symbol} - {datetime.now().strftime('%Y-%m-%d')}", # Should be set such that an asset can only be traded once per day
                                    isLeverage=0,
                                    orderFilter="Order",
                                )
                    print(f'{datetime.now(timezone.utc)} | Purchased {str(quantity)} of {symbol}')
                except Exception as e:
                    print('\nFailed to place order!\n')
                    print(f'ERROR: {e}')
            
            elif signal['action'] == 'Short':
                
                # Calculate take profit and stop loss
                take_profit = round(ask_price * (1-0.05) / tick_size) * tick_size
                stop_loss = round(ask_price * (1+0.05) / tick_size) * tick_size
                print(f'\nTake Profit={take_profit}, Stop Loss={stop_loss}\n')
                
                try:
                    # Short asset
                    sell_order = client.place_order(
                                    category="linear",
                                    symbol=symbol,
                                    side="Sell",
                                    orderType="Market",
                                    qty=f'{quantity:.{qty_prec}f}',              # TODO: Need to calculate | Also need to get coin precision
                                    price="15600",          # Market order will ignore this field
                                    takeProfit=f'{take_profit:.{precision}f}',
                                    stopLoss=f'{stop_loss:.{precision}f}',
                                    timeInForce="PostOnly",
                                    orderLinkId=f"{symbol} - {datetime.now().strftime('%Y-%m-%d')}", # Should be set such that an asset can only be traded once per day
                                    isLeverage=0,
                                    orderFilter="Order",
                                )
                    print(f'{datetime.now(timezone.utc)} | Purchased {str(quantity)} of {symbol}')
                except Exception as e:
                    print('\nFailed to place order!\n')
                    print(f'ERROR: {e}')
                    
    # if not close of bar: do nothing
    else:
        #TODO (IMPROVE): Print to a place holder that will always update without increasing the size of the log file.
        print(f'{dt} | Do nothing!')
    
    
# main function
def main():
    
    INTERVAL = 60
    SYMBOL = 'BTCUSDT'
    
    # Connect to websocket
    ws = WebSocket(testnet=False, channel_type="linear")
    print(f'{datetime.now(timezone.utc)} |Connected to websocket...')

    # subscribe to klines data (BTCUSDT)
    ws.kline_stream(interval=INTERVAL, symbol=SYMBOL, callback=handle_message)
    print(f'{datetime.now(timezone.utc)} |Subcribed to klines stream...')
    
    # sleep
    # sleep(60*60*24*7) # Sleep for 24 hours
    while True:
        sleep(1)
    
    # close websocket and stop process
    ws.exit()
    print('Done!')
    
if __name__ == '__main__':
    main()

