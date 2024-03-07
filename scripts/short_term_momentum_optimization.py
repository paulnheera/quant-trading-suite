import pandas as pd
import numpy as np
from scripts.data_download import get_bybit_data, get_bybit_asset_info
from scipy.optimize import brute
from time import sleep
from datetime import timedelta


INTERVAL = 60

interval = INTERVAL
# Define the strategy function
def strategy(data, threshold, sl, tp):
    trades = []
    profits = []
    in_position = False
    for index, row in data.iterrows():
        if not in_position and row['return'] > threshold:
            buyprice = row.Close
            
            entry_time = pd.to_datetime(index) + timedelta(minutes=interval * 1)
            entry_price = buyprice
            
            in_position = True
            trailing_stop = buyprice * (1-sl)
            take_profit = buyprice * (1+tp)
            continue
            
        if in_position:
            # check stop loss
            if row.Low <= trailing_stop:
                sellprice = trailing_stop
                exit_time = pd.to_datetime(index) + timedelta(minutes=interval * 1)
                exit_price = sellprice
                profit = (sellprice-buyprice)/buyprice - 0.0015
                trade = {'Entry Time':entry_time, 'Entry Price': entry_price, 'Exit Time':exit_time,'Exit Price':exit_price ,'Profit':profit}
                
                trades.append(trade)
                profits.append(profit)
                in_position = False
            # check take profit
            if row.High >= take_profit:
                sellprice = take_profit
                exit_time = pd.to_datetime(index) + timedelta(minutes=interval * 1)
                exit_price = sellprice
                profit = (sellprice-buyprice)/buyprice - 0.0015
                trade = {'Entry Time':entry_time, 'Entry Price': entry_price, 'Exit Time':exit_time,'Exit Price':exit_price ,'Profit':profit}
                
                trades.append(trade)
                profits.append(profit)
                in_position = False
            # when there is no trigger, update trailing stop
            if row.Close * (1-0.05) >= trailing_stop:
                trailing_stop = row.Close * (1-sl)
    
    total_profit = (1 + pd.Series(profits)).prod() - 1
                
    return trades

# Define the objective function to maximize
def objective_function(params, data):
    threshold, sl, tp = params
    trades = strategy(data, threshold, sl, tp)
    trades = pd.DataFrame(trades)
    total_profit = (1 + pd.Series(trades['Profit'])).prod() - 1 
    return -total_profit # Negative because we want to maximize

def process_symbol(symbol, start_time='2020-01-01 00:00:00'):
    
    # Download Data
    raw_data = get_bybit_data(product_type='linear', symbol=symbol, interval=60,
                      start_time=start_time,
                      verbose=False)
    
    # Data Preprocessing
    data = raw_data.set_index('Time')
    data['return'] = data.Close.pct_change()  # Calcualate return
    data = data.dropna() # drop na values
    
    # Strategy function
    # Objecive function
    
    # Define the ranges for the parameters
    threshold_range = slice(0.01, 0.05, 0.01)  # Example range for threshold
    sl_range = slice(0.01, 0.1, 0.01)          # Example range for sl
    tp_range = slice(0.01, 0.1, 0.01)          # Example range for tp
    
    # Use brute force optimization
    result = brute(objective_function, (threshold_range, sl_range, tp_range),args=(data,), full_output=True, finish=None)
    
    # Extract the optimized parameters and total profit
    optimized_params = result[0]
    best_total_profit = -result[1]  # Remember, we need to invert the sign back
    
    return {'Symbol':symbol, 
            'Threshold':optimized_params[0], 
            'Stop Loss':optimized_params[1], 
            'Take Profit':optimized_params[2],
            'Best Total Profit':round(best_total_profit,4)}

# Main code to process all symbols
if __name__ == "__main__":
    # Load symbol information
    df_info = get_bybit_asset_info()

    # Create an empty DataFrame to store the results
    results_df = pd.DataFrame(columns=['Symbol', 'Threshold', 'Stop Loss', 'Take Profit', 'Best Total Profit'])

    # Loop through each symbol
    for symbol in df_info['Symbol']:
        sleep(1) # Sleep to avoid exceed call rate-limit.
        try:
            # Call process_symbol function for each symbol
            result = process_symbol(symbol)
        
            # Store the results for the symbol in the DataFrame
            results_df =  pd.concat([results_df, pd.DataFrame(result, index=[0])], ignore_index=True)  
            #results_df.append(result, ignore_index=True)
        
            # Save the DataFrame to a CSV file after processing each symbol
            results_df.to_csv('results_so_far.csv', index=False)
        
            print(f"Processed symbol: {symbol}")
            
        except Exception as e:
            print(f"Error processing symbol {symbol}: {str(e)}")
            continue

    # Once all symbols are processed, save the final results DataFrame
    results_df.to_csv('final_results.csv', index=False)

