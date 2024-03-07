# Load Data

# import os
# print(os.getcwd())

# # change working directory
# os.chdir('C:\Repos\quant-trading-suite')
# print(os.getcwd())

import sys
sys.path.append('C:\\Repos\\quant-trading-suite')

# Import Libraries
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import sqlite3
from scripts.data_download import get_bybit_data

# def get_asset_info():
    
    
def round_down_to_interval(dt, interval):
    """
    Round down a datetime object to the nearest interval.
    """
    if interval < 1:
        raise ValueError("Interval must be at least 1 minute.")

    # Round down to the last complete interval
    minutes = dt.minute // interval * interval
    dt_rounded = dt.replace(minute=minutes, second=0, microsecond=0)

    # set to the start of the prevous bar (which completed at time dt_rounded above)
    dt_rounded -= timedelta(minutes=interval)

    return dt_rounded    

    
def get_table_status(table_name='KLINES_1H', exchange=None, product_type=None, symbol=None):
    
    con = sqlite3.connect('data/securities_master.db')
    c = con.cursor()
    
    # TODO: This query takes too long to run for the bigger tables!
    symbol_condition = f"WHERE Symbol = '{symbol}'" if symbol is not None else ''
    query = f"""
    SELECT 
        Exchange, 
        Product_Type, 
        Symbol,
        COUNT(*) as Count,
        MIN(Time) as First_time,
        MAX(Time) as Last_time,
        MAX(Load_time) as Last_load_time
    FROM {table_name}
    {symbol_condition}
    GROUP BY Exchange, Product_Type, Symbol
    """
    c.execute(query.replace('\n', ''))
    
    col_names = ['Exchange', 'Product_Type', 'Symbol', 'Count', 'First_time', 'Last_time', 'Last_load_time']
    
    df = pd.DataFrame(c.fetchall(), columns=col_names)
    
    return df

# if __name__ == '__main__':
#     status = get_table_status(table_name='KLINES_1H')

# Load data to database table
def load_to_db(df, exchange, product_type, symbol, interval):
    
    #IMPROVE: check that the interval is consistent with time in df
    
    interval_mapping = {1:'1M', 5:'5M', 15:'15M', 30:'30M',
                 60:'1H', 120:'2H', 240:'4H', 1440:'1D'}
    
    # create a sql connection
    con = sqlite3.connect('data/securities_master.db')
    
    # addtional columns
    df.insert(0, "Exchange", exchange)
    df.insert(1, "Product_type", product_type)
    df.insert(2, "Symbol", symbol)
    
    # load time
    load_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    df.insert(len(df.columns), "Load_time", load_time)
    
    # determine table name
    interval_ = interval_mapping.get(interval)
    table_name = f'KLINES_{interval_}'
    
    # load to database
    df.to_sql(table_name, con, if_exists='append', index=False)
    
    # close connection
    con.close()
    
    
def load_asset_info(df, exchange):
    
    # create a sql connection
    con = sqlite3.connect('data/securities_master.db')
    c = con.cursor()
    
    table_name = 'ASSET_INFO'
    
    # additional columns
    df.insert(0, "Exchange", exchange)
    
    # load time
    load_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    df.insert(len(df.columns), "Load_Time", load_time)
    
    # collect column table names
    c.execute(f'PRAGMA table_info({table_name})')
    columns = c.fetchall()
    column_names = [column[1] for column in columns]
    
    # select columns to upload
    df = df[column_names]
    
    # load to database
    df.to_sql(table_name, con, if_exists='append', index=False)

    
def update_db_data(exchange, product_type, symbol, interval, end_time=None):
    
    # get data status
    interval_mapping = {1:'1M', 5:'5M', 15:'15M', 30:'30M',
                 60:'1H', 120:'2H', 240:'4H', 1440:'1D'}
    interval_ = interval_mapping.get(interval)
    table_name = f'KLINES_{interval_}'
    status = get_table_status(table_name=table_name) #TODO: Fix this function as it currently takes to long to run.
    
    # set start time
    start_time = status['Last_time'].iloc[np.where((status['Symbol'] == symbol) & 
                                                   (status['Product_Type'] == product_type) &
                                                   (status['Exchange'] == exchange))].values
    
    if len(start_time) == 0:
        start_time = None
    else:
        start_time = start_time[0]
        
    print(f"{exchange} | {product_type} | {symbol} :Last time: {start_time}")
    
    if start_time is None:
        # get launch type of symbol
        start_time = datetime.strptime('2017-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    else:
        
        # add 1 time bar unit (add 1 hour)
        start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + timedelta(hours=1)
       
    print(f"{exchange} | {product_type} | {symbol} : Updating data from: {start_time}")
    t = start_time
        
    # set current time as the end time
    now = datetime.utcnow() 
    end_t = round_down_to_interval(now, interval) # round down to the last complete interval
    
    # Use while loop here for uploading data ---
    while t  < end_t:
        
        # calculate limit (time diff)
        t_diff = int((end_t - t).total_seconds() / 60 / interval) + 1
        
        print(f"{exchange} | {product_type} | {symbol} : Downloading data from: {t}")
        
        # download data up to endtime
        try:
            data = get_bybit_data(product_type=product_type, symbol=symbol, interval=interval,
                                  start_time=str(t),
                                  limit=min(1000,t_diff),
                                  verbose=True)
        except Exception as e:
            print(f'Error: {e}')
            
            print('Sleeping for 2 seconds...')
            time.sleep(2)
            
            continue
        
        # if data returns None then exit the function.
        if data is None:
            print(f"{exchange} | {product_type} | {symbol} : No Data.")
            return
            
        # load data to db
        load_to_db(data, exchange=exchange, product_type=product_type, symbol=symbol, interval=interval)
        
        t = data['Time'].iloc[-1] + timedelta( minutes=interval)
    
    # print outcome
    print('Update done!')
    
# if __name__ == '__main__':
#     update_db_data(exchange='Bybit', product_type='linear', symbol='BTCUSDT', interval=1440)
#     status = get_table_status()

def update_all_klines(symbols=None, interval=60):
    
    # create a sql connection
    con = sqlite3.connect('data/securities_master.db')
    c = con.cursor()
    
    print('fetching all symbols...')
    query = """ SELECT DISTINCT Symbol FROM ASSET_INFO """
    c.execute(query)
    symbols = c.fetchall()
    symbols = [s[0] for s in symbols]
    
    
    print('start updating data for each symbol')
    for symbol in symbols:
        
        # update current symbols
        update_db_data(exchange='Bybit', product_type='linear', symbol=symbol, interval=interval)
        
if __name__ == '__main__':
    update_all_klines(interval=1)
    
    

    
    
        
    
    


    
    


