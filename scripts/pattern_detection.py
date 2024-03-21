# Patterns

# Load Libraries
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
import matplotlib.pyplot as plt

from scripts.data_download import get_bybit_data


#%% Inflexion Points
def inflexion_points(data, order=10, col='Close'):
    """
    Creates two columnes local_max, local_min indicating if the 
    price point is a local maximum or minimun with a 0 or 1.

    Parameters
    ----------
    data : dataframe or series
        OHLCV dataframe
    order : TYPE, optional
        DESCRIPTION. The default is 10.
    col : TYPE, optional
        DESCRIPTION. The default is 'Close'.

    Returns
    -------
    None.

    """
    
    y = data[col].values # Converts to array
    
    # Find out relative local extrema:
    max_ind = list(argrelextrema(y, np.greater, order=order)[0])
    min_ind = list(argrelextrema(y, np.less, order=order)[0])
    
    # Create columns to indicate local maxima and minima
    data['local_max'] = 0
    data['local_min'] = 0
    
    data.loc[max_ind, 'local_max'] = 1
    data.loc[min_ind, 'local_min'] = 1
    
    return data

#%% Label Inflexion Points

def label_points(result):
    
    local_max_df = result[result['local_max'] == 1]
    local_min_df = result[result['local_min'] == 1]
    
    local_max_df['label'] = np.where(local_max_df['Close'] > local_max_df['Close'].shift(1), 'HH', 'LH')
    local_min_df['label'] = np.where(local_min_df['Close'] < local_min_df['Close'].shift(1), 'LL', 'HL')
    
    labeled_points = pd.concat([local_max_df[['Time', 'label']], local_min_df[['Time', 'label']]])
    
    labeled_points.sort_values('Time', inplace=True, ignore_index=True)
    
    return labeled_points
    

#%%
if __name__ == '__main__':
    # Download Data
    raw_data = get_bybit_data(product_type='linear', symbol='SOLUSDT', interval=60,
                      start_time='2024-03-01 00:00:00',
                      verbose=False)
    
    # Add columns for local maxima and minima
    result = inflexion_points(raw_data, order=5)
    
    result = result.iloc[-500:] # The last 100 points
    
    # Plot the closing price data
    plt.figure(figsize=(10, 6))
    plt.plot(result['Close'], label='Close Price', color='blue')
    
    # Plot local maxima and minima points
    local_max_points = result.loc[result['local_max'] == 1, 'Close']
    local_min_points = result.loc[result['local_min'] == 1, 'Close']
    plt.scatter(local_max_points.index, local_max_points, color='red', label='Local Maxima')
    plt.scatter(local_min_points.index, local_min_points, color='green', label='Local Minima')
    
    plt.title('Closing Price with Local Maxima and Minima')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.show()
