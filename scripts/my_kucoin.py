# Kucoin Market Data

import pandas as pd
import numpy as np

from kucoin.client import Market

client = Market(url='https://api.kucoin.com')


#SYMBOL = 'ZEND-USDT'
#symbol = SYMBOL

# get ask_price
def get_ask(symbol):
    res = client.get_ticker(symbol)
    ask_price = res.get('bestAsk')

    return ask_price



# Get trades history
def get_trades(symbol):
    res = client.get_trade_histories(symbol)
    df = pd.DataFrame(res)
    return df

    # Get order book
def get_order_book(symbol):
    res = client.get_part_order(100,symbol)
    df = pd.DataFrame(res)
    return df
    