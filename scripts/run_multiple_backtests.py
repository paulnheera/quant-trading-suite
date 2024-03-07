# Run Multiple Backtests

#%% Import libraries
import sys
sys.path.append('C:\\Repos\\quant-trading-suite')

from scripts.BacktestBase import *
from scripts.BacktestLongShort import * 
import talib as ta
import pandas as pd
import numpy as np
import sqlite3

#%% Retrieve assets

con = sqlite3.connect('data/securities_master.db')
c = con.cursor()

query = "SELECT Symbol FROM ASSET_INFO WHERE Launch_Time < '2023-01-01 00:00:00'"
c.execute(query)

assets = [asset[0] for asset in c.fetchall()]


results = []

# Run backtest for each assets over a defined period
for asset in assets:
    
    lsbt = BacktestLongShort(exchange='bybit',
                             symbol=asset,
                             interval=15,
                             start='2023-01-01 00:00:00',
                             end='2023-10-31 12:00:00',
                             amount=10000,
                             ptc=0.0012,
                             enable_stop_orders=True,
                             enable_filter=True,
                             sl=0.04,
                             tp=None)
    
    lsbt.run_sma_strategy(50, 400)
    
    perf = ((lsbt.amount - lsbt.initial_amount) / 
            lsbt.initial_amount * 100)
    
    temp = {'symbol':lsbt.symbol, 'performance':perf}
    
    results.append(temp)
    
    print('Done!')
    
    
# Plot results
df_results = pd.DataFrame(results)
df_results = df_results.sort_values('performance', ascending=False)

plt.figure(figsize=(10,6))
plt.bar(x=df_results['symbol'], height=df_results['performance'])
plt.ylabel('Performance')
plt.title('Performance by Symbol')
plt.xticks([])
plt.show()


# Run specific backtest
strat = BacktestLongShort(exchange='bybit',
                         symbol='ACHUSDT',
                         interval=15,
                         start='2023-01-01 00:00:00',
                         end='2023-10-31 12:00:00',
                         amount=10000,
                         ptc=0.0012,
                         enable_stop_orders=True,
                         enable_filter=True,
                         sl=0.04,
                         tp=None)

bnh = BacktestLongShort(exchange='bybit',
                         symbol='ACHUSDT',
                         interval=15,
                         start='2023-01-01 00:00:00',
                         end='2023-10-31 12:00:00',
                         amount=10000,
                         ptc=0.0012)


strat.run_sma_strategy(50, 400)
bnh.run_buy_and_hold()

fig, axs = plt.subplots(2, gridspec_kw={'height_ratios': [2, 1]}, sharex=True)
strat.plot_equity(ax=axs[0])
bnh.plot_equity(ax=axs[0])
strat.plot_drawdowns(ax=axs[1])

    

