
#%% Import Libraries

# Import libraries
import pandas as pd
import numpy as np
import time
from datetime import datetime
from datetime import timezone
from pybit.unified_trading import HTTP

#%% Connect to API
 
# Connect to API
client = HTTP()

#%% Retreive list of tradable pairs

# get tickers for all symbols
raw = client.get_tickers(category="spot")
ls_symbols = raw['result']['list']
df_symbols = pd.DataFrame(ls_symbols)

df_tickers = df_symbols[['symbol', 'bid1Price', 'bid1Size','ask1Price','ask1Size']]

# get asset infomation for all symbols
raw_info = client.get_instruments_info(category='spot')
ls_info = raw_info['result']['list']
df_info = pd.DataFrame(ls_info)

df_info = df_info[['symbol', 'baseCoin', 'quoteCoin', 'status']]

# combine tickers and symbol info
df_pairs = df_tickers.merge(df_info, on='symbol')

#%% Functions

def structure_triangular_pairs(df_pairs):
    
    triangular_pairs_list = []
    remove_duplicates_list = []
    
    ls_pairs = df_pairs.to_dict(orient='records')

    for pair_a in ls_pairs:
        
        a_base = pair_a['baseCoin']
        a_quote = pair_a['quoteCoin']
        pair_a = pair_a['symbol']

        a_pair_box = [a_base, a_quote]

        for pair_b in ls_pairs:
            b_base = pair_b['baseCoin']
            b_quote = pair_b['quoteCoin']
            pair_b = pair_b['symbol']

            if pair_b != pair_a:
                if b_base in a_pair_box or b_quote in a_pair_box:

                    for pair_c in ls_pairs:
                        c_base = pair_c['baseCoin']
                        c_quote = pair_c['quoteCoin']
                        pair_c = pair_c['symbol']

                        if pair_c != pair_a and pair_c != pair_b:
                            combine_all = [pair_a, pair_b, pair_c]
                            pair_box = [a_base, a_quote, b_base, b_quote, c_base, c_quote]
                            counts_c_base = 0
                            for i in pair_box:
                                if i == c_base:
                                    counts_c_base += 1

                            counts_c_quote = 0
                            for i in pair_box:
                                if i == c_quote:
                                    counts_c_quote += 1

                            if counts_c_base == 2 and counts_c_quote == 2 and c_base != c_quote:
                                combined = pair_a + ',' + pair_b + ',' + pair_c
                                unique_item = ''.join(sorted(combine_all))
                                if unique_item not in remove_duplicates_list:
                                    match_dict = {
                                        "a_base": a_base,
                                        "b_base": b_base,
                                        "c_base": c_base,
                                        "a_quote": a_quote,
                                        "b_quote": b_quote,
                                        "c_quote": c_quote,
                                        "pair_a": pair_a,
                                        "pair_b": pair_b,
                                        "pair_c": pair_c,
                                        "combined": combined
                                    }
                                    triangular_pairs_list.append(match_dict)
                                    remove_duplicates_list.append(unique_item)
    
    return triangular_pairs_list

if __name__ == '__main__':
    tri_pairs = structure_triangular_pairs(df_pairs)
    
    t_pair = tri_pairs[0]

#%%

def get_price_for_t_pair(t_pair, df_tickers):
    pair_a = t_pair['pair_a']
    pair_b = t_pair['pair_b']
    pair_c = t_pair['pair_c']

    pair_a_ask = df_tickers.iloc[np.where(df_tickers['symbol'] == pair_a)]['ask1Price'].values[0]
    pair_a_bid = df_tickers.iloc[np.where(df_tickers['symbol'] == pair_a)]['bid1Price'].values[0]
    
    pair_b_ask = df_tickers.iloc[np.where(df_tickers['symbol'] == pair_b)]['ask1Price'].values[0]
    pair_b_bid = df_tickers.iloc[np.where(df_tickers['symbol'] == pair_b)]['bid1Price'].values[0]
    
    pair_c_ask = df_tickers.iloc[np.where(df_tickers['symbol'] == pair_c)]['ask1Price'].values[0]
    pair_c_bid = df_tickers.iloc[np.where(df_tickers['symbol'] == pair_c)]['bid1Price'].values[0]
    
    return {
        "pair_a_ask": float(pair_a_ask),
        "pair_a_bid": float(pair_a_bid),
        "pair_b_ask": float(pair_b_ask),
        "pair_b_bid": float(pair_b_bid),
        "pair_c_ask": float(pair_c_ask),
        "pair_c_bid": float(pair_c_bid)
    }

if __name__ == '__main__':
    prices_dict = get_price_for_t_pair(t_pair, df_tickers=df_tickers)
    

#%%
def cal_triangular_arb_surface_rate(t_pair, prices_dict):

    starting_amount = 1
    min_surface_rate = 0 
    surface_dict = {}
    contract_1 = ""
    contract_2 = ""
    contract_3 = ""
    direction_trade_1 = ""
    direction_trade_2 = ""
    direction_trade_3 = ""
    acquired_coin_t2 = 0
    acquired_coin_t3 = 0
    calculated = 0

    a_base = t_pair['a_base']
    a_quote = t_pair['a_quote']
    b_base = t_pair['b_base']
    b_quote = t_pair['b_quote']
    c_base = t_pair['c_base']
    c_quote = t_pair['c_quote']
    pair_a = t_pair['pair_a']
    pair_b = t_pair['pair_b']
    pair_c = t_pair['pair_c']

    a_ask = prices_dict['pair_a_ask']
    a_bid = prices_dict['pair_a_bid']
    b_ask = prices_dict['pair_b_ask']
    b_bid = prices_dict['pair_b_bid']
    c_ask = prices_dict['pair_c_ask']
    c_bid = prices_dict['pair_c_bid']

    direction_list = ['forward', 'reverse'] # forward and backward?
    for direction in direction_list:

        swap_1 = 0
        swap_2 = 0
        swap_3 = 0
        swap_1_rate = 0
        swap_2_rate = 0
        swap_3_rate = 0

        # Assume starting with a_base and swapping for a_quote
        if direction == "forward":
            swap_1 = a_base
            swap_2 = a_quote
            swap_1_rate = a_bid # Sell base @ bid price to get an amount of the quote asset
            direction_trade_1 = "base_to_quote"

        # Assume starting with a_quote and swapping for a_base
        if direction == "reverse":
            swap_1 = a_quote
            swap_2 = a_base
            swap_1_rate = 1 / a_ask # Buy base asset, but @ ask price. amount = the inverse of the quote
            direction_trade_1 = "quote_to_base"

        # Place first trade
        contract_1 = pair_a
        acquired_coin_t1 = starting_amount * swap_1_rate

        """  FORWARD """
        # SCENARIO 1
        if direction == "forward":
            if a_quote == b_quote and calculated == 0: # what does calculated do?
                swap_2_rate = 1 / b_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_b

                if b_base == c_base:
                    swap_3 = c_base
                    swap_3_rate = c_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                if b_base == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 2
        if direction == "forward":
            if a_quote == b_base and calculated == 0:
                swap_2_rate = b_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_b

                if b_quote == c_base:
                    swap_3 = c_base
                    swap_3_rate = c_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                if b_quote == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 3
        if direction == "forward":
            if a_quote == c_quote and calculated == 0:
                swap_2_rate = 1 / c_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_c

                if c_base == b_base:
                    swap_3 = b_base
                    swap_3_rate = b_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                if c_base == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 4
        if direction == "forward":
            if a_quote == c_base and calculated == 0:
                swap_2_rate = c_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_c

                if c_quote == b_base:
                    swap_3 = b_base
                    swap_3_rate = b_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                if c_quote == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1




        """ REVERSE """
        # SCENARIO 1
        if direction == "reverse":
            if a_base == b_quote and calculated == 0:
                swap_2_rate = 1 / b_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_b

                if b_base == c_base:
                    swap_3 = c_base
                    swap_3_rate = c_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                if b_base == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 2
        if direction == "reverse":
            if a_base == b_base and calculated == 0:
                swap_2_rate = b_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_b

                if b_quote == c_base:
                    swap_3 = c_base
                    swap_3_rate = c_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                if b_quote == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 3
        if direction == "reverse":
            if a_base == c_quote and calculated == 0:
                swap_2_rate = 1 / c_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_c

                if c_base == b_base:
                    swap_3 = b_base
                    swap_3_rate = b_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                if c_base == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 4
        if direction == "reverse":
            if a_base == c_base and calculated == 0:
                swap_2_rate = c_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_c

                if c_quote == b_base:
                    swap_3 = b_base
                    swap_3_rate = b_bid
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                if c_quote == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        """ PROFIT LOSS OUTPUT"""
        # Profit and Loss Calc
        profit_loss = acquired_coin_t3 - starting_amount
        profit_loss_perc = (profit_loss / starting_amount) * 100 if profit_loss != 0 else 0 #!! Do I need the if statement here.

        # Trade Description
        trade_description_1 = f"Start with {swap_1} of {starting_amount}. Swap at {swap_1_rate} for {swap_2} acquiring {acquired_coin_t1}."
        trade_description_2 = f"Swap {acquired_coin_t1} of {swap_2} at {swap_2_rate} for {swap_3} acquiring {acquired_coin_t2}."
        trade_description_3 = f"Swap {acquired_coin_t2} of {swap_3} at {swap_3_rate} for {swap_1} acquiring {acquired_coin_t3}."
        
        print(f'PnL = {profit_loss_perc}')
        
        # Output Results
        if profit_loss_perc > min_surface_rate:
        # if True:
            surface_dict = {
                "swap_1": swap_1,
                "swap_2": swap_2,
                "swap_3": swap_3,
                "contract_1": contract_1,
                "contract_2": contract_2,
                "contract_3": contract_3,
                "direction_trade_1": direction_trade_1,
                "direction_trade_2": direction_trade_2,
                "direction_trade_3": direction_trade_3,
                "starting_amount": starting_amount,
                "acquired_coin_t1": acquired_coin_t1,
                "acquired_coin_t2": acquired_coin_t2,
                "acquired_coin_t3": acquired_coin_t3,
                "swap_1_rate": swap_1_rate,
                "swap_2_rate": swap_2_rate,
                "swap_3_rate": swap_3_rate,
                "profit_loss": profit_loss,
                "profit_loss_perc": profit_loss_perc,
                "direction": direction,
                "trade_description_1": trade_description_1,
                "trade_description_2": trade_description_2,
                "trade_description_3": trade_description_3
            }

            return surface_dict

    return surface_dict

if __name__ == '__main__':
    
    surface_dict = cal_triangular_arb_surface_rate(t_pair, prices_dict)

#%%
    
# Get available triangle arbitrage pairs
# tri_pairs = structure_triangular_pairs(df_pairs)

# get tickers for all symbols
raw = client.get_tickers(category="spot")
ls_symbols = raw['result']['list']
df_symbols = pd.DataFrame(ls_symbols)
df_tickers = df_symbols[['symbol', 'bid1Price', 'bid1Size','ask1Price','ask1Size']]

for t_pair in tri_pairs:
    prices_dict = get_price_for_t_pair(t_pair, df_tickers)
    surface_arb = cal_triangular_arb_surface_rate(t_pair, prices_dict)
    if len(surface_arb) > 0: 
        print(surface_arb)
            






