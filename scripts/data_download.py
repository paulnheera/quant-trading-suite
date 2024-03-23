# data download


# Import Libraries
# Import libraries
import pandas as pd
import time
from datetime import datetime
from datetime import timezone


from pybit.unified_trading import HTTP
import binance
from binance import Client

#%%
# Function to download data from bybit
def get_bybit_data(product_type, symbol, interval, start_time, end_time=None, limit=None, verbose=False):
    
    # Connect to API
    client = HTTP()
    
    ''' Download data from exchange 
    
    Parameters
    ==========
    product_type: string
        product type. spot, liniear, inverse
    symbol: string
        asset symbol
    interval: int
        timeframe interval in minutes
    start_time: string
        Starting time of data
    end_time: string
        End point of data
    '''
    
    # transform interval
    interval_mapping = {1:1, 5:5, 15:15, 30:30,
                 60:60, 120:120, 240:240, 1440:'D'}
    interval_ = interval_mapping.get(interval)
    
    # Check that time variables are string
    
    # Convert time variables to timestamps
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    start_ts = int(start_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 # convert timestamp to ms
    
    # Set end_time to current time if None is provided
    if end_time is None:
        end_time = datetime.now()
        end_ts = int(end_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 # convert timestamp to ms
    else:
        end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        end_ts = int(end_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 # convert timestamp to ms
    
    # set main list object & current time cursor
    raw_ls = []
    c_ts = start_ts
    
    if limit is not None:
        
        # download the limited amount of bars from the specified start time
        temp = client.get_kline(
                product_type=product_type,
                symbol=symbol,
                interval=str(interval_),
                start=c_ts,
                limit=limit
        )
        ## ERROR: If there is no more data after a specific time.
        
        # append list to main list
        temp = temp['result']['list']
        raw_ls.extend(temp)
        
        if len(temp) == 0:
            return None
    
    else:
    
        # while loop to download several pages of data
        while c_ts < end_ts:
            
            # time difference
            diff = int( (end_ts - c_ts) / (interval*60*1000) )    # divide by 60 to get to mins, divide by interval to get to frequency (in mins)
            
            if verbose:
                print("Downloading data from {}".format(pd.to_datetime(c_ts, unit='ms')))
                print("{} bars remaining...".format(diff))
                print("-"*55)
            
            # download data
            temp = client.get_kline(
                    product_type=product_type,
                    symbol=symbol,
                    interval=str(interval_),
                    start=c_ts,
                    limit=min(1000,diff)
    
            )
            
            # append list to main list
            temp = temp['result']['list']
            raw_ls.extend(temp)
            
            if len(temp) == 0:
                break
        
            # update time cursor
            #print(f'Last bar: {pd.to_datetime(temp[0][0], unit="ms")}')
            c_ts = int(temp[0][0]) + interval*60*1000 # add one interval to the last startTime - interval (m)
            #print(f'Next bar: {pd.to_datetime(c_ts, unit="ms")}')
            
            ##IMPROVE: CREATE AN EXCEPTION FOR WHEN c_ts IS GREATER THAN THE AVAILABLE TIME.
            ## I.E. WHERE IT CANNOT BE USED AS VALUE FOR THE ARGUMENT from_time.
            
            ##IMPROVE: WHEN WE SET A START_TIME AND END_TIME BEFORE LAUNCH TIME IT WILL STILL 
            ## DOWNLOAD DATA FROM THE LAUCH TIME (FOR 1000 BARS)
            
            # sleep for a bit
            time.sleep(1)
             
    # convert to dataframe
    df = pd.DataFrame(raw_ls)
        
    df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']
    
    # Convert fields to numerical values
    df[df.columns] = df[df.columns].apply(pd.to_numeric)  
    
    # Convert time column to datetime
    df['Time'] = pd.to_datetime(df['Time']/1000, unit='s')

    # Sort the data by Time
    df = df.sort_values('Time', ignore_index=True)
    
    # Select columns
    cols = ['Time', 'Open','High','Low', 'Close','Volume']
    df = df[cols]
    
    # return ohlcv dataframe
    return df

#%%
if __name__ == '__main__':
    
    data = get_bybit_data(product_type='linear', symbol='BTCUSDT', interval=1440,
                          start_time='2017-01-01 00:00:00',
                          end_time='2017-12-31 23:59:00',
                          verbose=True)
    
    data = get_bybit_data(product_type='linear', symbol='BTCUSDT', interval=60,
                          start_time='2021-01-01 00:00:00', limit=2000,
                          verbose=True)
    
    data = get_bybit_data(product_type='linear', symbol='MCUSDT', interval=60,
                          start_time='2023-11-07 00:00:00', limit=1000,
                          verbose=True)
        
    data['Close'].plot(figsize=(10,6), title='BTCUSDT')

#%%
def get_binance_data(symbol, interval, start_time, end_time=None, product_type=None, limit=None, verbose=False):
        
    ''' Download data from Binance Exchange
    
    Parameters
    ==========
    product_type: string
        product type. spot, liniear, inverse
    symbol: string
        asset symbol
    interval: int
        timeframe interval in minutes
    start_time: string
        Starting time of data
    end_time: string
        End point of data
    '''
    
    # Connect to API
    client = Client()
    
    # Transform interval
    interval_mapping = {1:client.KLINE_INTERVAL_1MINUTE, 
                        5:client.KLINE_INTERVAL_5MINUTE, 
                        15:client.KLINE_INTERVAL_15MINUTE,
                        30:client.KLINE_INTERVAL_30MINUTE,
                        60:client.KLINE_INTERVAL_1HOUR, 
                        120:client.KLINE_INTERVAL_2HOUR, 
                        240:client.KLINE_INTERVAL_4HOUR,
                        1440:client.KLINE_INTERVAL_1DAY}
    interval_ = interval_mapping.get(interval)
    
    # Convert time variables to timestamps
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    start_ts = int(start_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 # convert timestamp to ms
    
    # Set end_time to current time if None is provided
    if end_time is None:
        end_time = datetime.now()
        end_ts = int(end_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 # convert timestamp to ms
    else:
        end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        end_ts = int(end_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 # convert timestamp to ms
    
    # set main list object & current time cursor
    raw_ls = []
    c_ts = start_ts

    # download the limited amount of bars from the specified start time
    raw_ls = client.get_historical_klines(
            klines_type=binance.enums.HistoricalKlinesType.SPOT,
            symbol=symbol,
            interval=interval_,
            start_str=c_ts,             # start date string in UTC format or timestamp in milliseconds
            end_str=end_ts
    )
        
    # convert to dataframe
    df = pd.DataFrame(raw_ls)
        
    df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Turnover',
                  'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume',
                  'Ignore']
    
    # Convert fields to numerical values
    df[df.columns] = df[df.columns].apply(pd.to_numeric)  
    
    # Convert time column to datetime
    df['Time'] = pd.to_datetime(df['Time'], unit='ms')

    # Sort the data by Time
    df = df.sort_values('Time', ignore_index=True)
    
    # Select columns
    cols = ['Time', 'Open','High','Low', 'Close','Volume']
    df = df[cols]
    
    # return ohlcv dataframe
    return df

if __name__ == '__main__':
    data = get_binance_data(symbol='BTCUSDT', interval=60,
                          start_time='2017-01-01 00:00:00',
                          end_time='2017-12-31 23:59:00',
                          verbose=True)

#%%
        
# To Do:
    # Download data for the first time.

# Fuction to download data from kucoin 

# Function to download data from luno

# Function to download instrument information

def get_bybit_asset_info(product_type="linear"):
    
    # Connect to API
    client = HTTP()
    
    # Get all symbols
    raw = client.get_instruments_info(category=product_type)
    
    # Convert to dataframe
    result = pd.DataFrame(raw['result']['list'])
    
    # Split out leverageFilter column
    leverage_filter = pd.json_normalize(result['leverageFilter'])
    
    # Split out priceFilter column
    price_filter = pd.json_normalize(result['priceFilter'])
    
    # Split out lotSizeFilter column
    lot_size_filter = pd.json_normalize(result['lotSizeFilter'])
    
    # Join dataframes from split out columns:
    result = pd.concat([result, leverage_filter, price_filter, lot_size_filter], axis=1)
    result = result.drop(columns=['leverageFilter', 'priceFilter', 'lotSizeFilter'])
    
    
    # trim the number of columns to necessary columns only
    cols = ['symbol', 'contractType', 'status', 'baseCoin', 'quoteCoin',
           'launchTime', 'deliveryTime', 'minPrice', 'maxPrice',
           'tickSize', 'maxOrderQty', 'minOrderQty', 'qtyStep']
    
    df = result[cols]
    
    df = df.rename(columns={'symbol':'Symbol',
                            'contractType':'Product_Type',
                            'status':'Status',
                            'baseCoin':'Base_Asset',
                            'quoteCoin':'Quote_Asset',
                            'launchTime':'Launch_Time',
                            'deliveryTime':'Delivery_Time',
                            'minPrice':'Min_Price',
                            'maxPrice':'Max_Price',
                            'tickSize':'Tick_Size',
                            'maxOrderQty':'Max_Qty',
                            'minOrderQty':'Min_Qty',
                            'qtyStep':'Qty_Step'})
    
    # Convert timestamps to datetime
    df['Launch_Time'] = pd.to_datetime(df['Launch_Time'], unit='ms')
    df['Delivery_time'] = pd.to_datetime(df['Delivery_Time'], unit='ms')
    
    
    # return dataframe of asset/instrument info
    return df

if __name__ == '__main__':
    
    df_asset_info = get_bybit_asset_info()
    
    



