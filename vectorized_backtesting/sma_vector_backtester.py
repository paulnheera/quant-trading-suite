# SMA Vectorized Backtesting 

import numpy as np
import pandas as pd
from scipy.optimize import brute
from scripts.data_download import get_bybit_data
from pylab import mpl, plt
plt.style.use('seaborn')
mpl.rcParams['font.family'] = 'serif'


class SMAVectorBacktester():
    
    def __init__(self, symbol, SMA1, SMA2, interval, start_time, end_time=None, data=None):
        self.symbol = symbol # could be `pair`
        self.SMA1 = SMA1
        self.SMA2 = SMA2
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.results = None
        self.get_data(data)
        
    def get_data(self, data=None):
        """
        Retrieves and prepares the data.
        """
        
        if data is not None:
            self.data = data
            return
            
        raw = get_bybit_data(product_type='linear', symbol=self.symbol, interval=self.interval,
                              start_time=self.start_time, end_time=self.end_time,
                              verbose=True)
        
        raw = raw.set_index('Time') # Set Time as the index of the data frame.
        
        raw['return'] = np.log(raw['Close']/ raw['Close'].shift(1))
        raw['SMA1'] = raw['Close'].rolling(self.SMA1).mean()
        raw['SMA2'] = raw['Close'].rolling(self.SMA2).mean()
        
        self.data = raw.copy()
        
    def plot_data(self):
        self.data[['Close', 'SMA1', 'SMA2']].plot(title=f"{self.symbol} - {self.interval}M | {self.SMA1} & {self.SMA2} SMA's",
                                                  figsize=(10, 6))
        
    def set_parameters(self, SMA1=None, SMA2=None):
        
        if SMA1 is not None:
            self.SMA1 = SMA1
            self.data['SMA1'] = self.data['Close'].rolling(self.SMA1).mean()
        if SMA2 is not None:
            self.SMA2 = SMA2
            self.data['SMA2'] = self.data['Close'].rolling(self.SMA2).mean()
            
    def run_strategy(self):
        """ Backtests the trading strategy
        """
        data = self.data.copy().dropna()
        data['position'] = np.where(data['SMA1'] > data['SMA2'], 1, -1)
        data['strategy'] = data['position'].shift(1) * data['return']
        data.dropna(inplace=True)
        data['creturns'] = data['return'].cumsum().apply(np.exp)
        data['cstrategy'] = data['strategy'].cumsum().apply(np.exp)
        self.results = data
        # gross performance of the strategy
        aperf = data['cstrategy'].iloc[-1]
        # out-/underperformance of strategy
        operf = aperf - data['creturns'].iloc[-1]
        
        # drawdowns
        data['cummax'] = data['cstrategy'].cummax()
        drawdown = data['cummax'] - data['cstrategy'] # 1.5  - 1 = 0.5 (This is not really a 50% drawdown)
        max_dd = drawdown.max()
        
        return round(aperf, 2), round(operf, 2), round(max_dd, 2) # Return the absolute perfomance and out-performance as a tuple.
    
    def plot_results(self):
        """ Plots the cumulative performance of the trading strategy
        compared to the symbol (buy and hold strategy)
        """
        print("Plotting...")
        if self.results is None:
            print('No results to plot yet. Run a strategy.')
        
        title = f"{self.symbol} | SMA1={self.SMA1}, SMA2={self.SMA2}"
        self.results[['creturns', 'cstrategy']].plot(title=title, 
                                                     figsize=(10,6))
    
    def update_and_run(self, SMA):
        """ Updates SMA parameters and returns negative absolute performance
        (for minimization algorithm)
        
        Parameters
        ==========
        SMA: tuple
            SMA parameter tuple
        """
        self.set_parameters(int(SMA[0]), int(SMA[1]))
        return -self.run_strategy()[0] # Return the negative absolute performance
    
    def update_and_run_dd(self, SMA):
        """ Updates SMA parameters and returns the maxmimum drawdown
        (for minimization algorithm)
        
        Parameters
        ==========
        SMA: tuple
        """
        self.set_parameters(int(SMA[0]), int(SMA[1]))
        return self.run_strategy()[2]

    
    def optimize_parameters(self, SMA1_range, SMA2_range, metric='return'):
        """ Finds global maximum/minimum given the SMA parameter ranges.
        Parameters
        ==========
        SMA1_range, SMA2_range: tuple
            tuples of the form (start, end, step size)
        """
        if metric == 'return':
            opt = brute(self.update_and_run, (SMA1_range, SMA2_range), finish=None)
            return opt, -self.update_and_run(opt)
        elif metric =='drawdown':
            opt = brute(self.update_and_run_dd, (SMA1_range, SMA2_range), finish=None)
            return opt, self.update_and_run_dd(opt)
    
    ##TODO: Add strat_start_time and strat_end_time (for reports)
            
    
#%%

if __name__ == "__main__":
    smavbt = SMAVectorBacktester(symbol="BTCUSDT", SMA1=42, SMA2=252, interval=60, start_time='2024-01-01 00:00:00', data=data)
    smavbt.plot_data()
    
    print(smavbt.run_strategy())
    smavbt.set_parameters(SMA1=20, SMA2=100)
    print(smavbt.run_strategy())
    
    # Optimize return
    print(smavbt.optimize_parameters((30,56,4), (100,300,4)))
    
    smavbt.set_parameters(SMA1=54, SMA2=100)
    print(smavbt.run_strategy())
    
    # Optimize drawdown
    print(smavbt.optimize_parameters((30,56,4), (100,300,4), metric='drawdown'))
    
    smavbt.set_parameters(SMA1=42, SMA2=112)
    print(smavbt.run_strategy())
    smavbt.plot_results()
    
    smavbt.plot_results()
    
    
    
    
        
        