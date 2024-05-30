# BacktestLongShort

#%% Libraries
from scripts.BacktestBase import *
#import talib as ta

#%% BacktestLongShort Class

class BacktestLongShort(BacktestBase):
        
    def go_long(self, bar, units=None, amount=None, sl=None, tp=None):
        if self.position == -1:
            self.place_buy_order(bar, units=-self.units) # Close existing short position!
        if units:
            self.place_buy_order(bar, units=units, sl=sl, tp=tp)
        elif amount:
            if amount == 'all':
                amount = self.amount
            self.place_buy_order(bar, amount=amount, sl=sl, tp=tp)
            
    def go_short(self, bar, units=None, amount=None, sl=None, tp=None):
        if self.position == 1:
            self.place_sell_order(bar, units=self.units) # Close existing long position!
        if units:
            self.place_sell_order(bar, units=units, sl=sl, tp=tp)
        elif amount:
            if amount == 'all':
                amount = self.amount
            self.place_sell_order(bar, amount=amount, sl=sl, tp=tp)
            
    def run_sma_strategy(self, SMA1, SMA2, enable_filter=False):
        
        # Print strategy information
        msg = f'\nRunning SMA strategy | SMA1={SMA1} & SMA2={SMA2}'
        msg += f'\nfrom: {self.start} to: {self.end}'
        msg += f'\nfixed costs {self.ftc} | '
        msg += f'proportional costs {self.ptc}'       
        if self.verbose:
            print(msg)
            print('=' * 55)
        
        # Reset backtest variables
        self.position = 0 # initial netural position
        self.trades = 0 # no of trades yet
        self.amount = self.initial_amount # reset initial capital
        self.results = [] # reset results dictionary
        self.order_history = [] # reset order_history
        
        # Make a copy of the data
        data = self.data.copy()
        
        # Calculate required indicators
        data['return'] = data['Close']/ data['Close'].shift(1) - 1
        data['SMA1'] = data['Close'].rolling(SMA1).mean() # For strategy signal
        data['SMA2'] = data['Close'].rolling(SMA2).mean() # For strategy signal
        #data['ADX'] = ta.ADX(data['High'], data['Low'], data['Close']) # Filter indicator
        
        start_bar = data.index.get_loc(self.start)
        end_bar = data.index.get_loc(self.end)
        
        for bar in range(start_bar, end_bar + 1):
            
            # Check for any stop orders
            if self.enable_stop_orders == True:
                self.check_stop_loss(bar=bar)
                self.check_take_profit(bar=bar)
                
            # Calculate Filters
            if enable_filter:
                long_filter = data['ADX'].iloc[bar-1] > 25
                short_filter = data['ADX'].iloc[bar-1] > 25
            else:
                long_filter = short_filter = True
                
            # Check for Long entry signal
            if self.position in [0, -1]:
                if ((data['SMA1'].iloc[bar] > data['SMA2'].iloc[bar] 
                    and data['SMA1'].iloc[bar-1] <= data['SMA2'].iloc[bar-1]) 
                    and long_filter):

                    self.go_long(bar, amount='all', sl=self.sl, tp=self.tp)
                    self.position = 1 # long position
                    if self.verbose:
                        print('-' * 55)
                          
            # Check for Short entry signal
            if self.position in [0, 1]:
                if ((data['SMA1'].iloc[bar] < data['SMA2'].iloc[bar]
                    and data['SMA1'].iloc[bar-1] >= data['SMA2'].iloc[bar-1]
                    ) and short_filter):
                    
                    self.go_short(bar,amount='all', sl=self.sl, tp=self.tp)
                    self.position = -1 # short position
                    if self.verbose:
                        print('-' * 55)
            
            self.update_results(bar)
            
            #self.update_trailling_sl(bar)
        
        # Close out any open trades at the end of the backtest
        self.close_out(bar)
         
    def run_channel_breakout_strategy(self, x, y):
        '''
        Channel Breakout Rules
        -----------------------
        Entry:
            Buy if current bar's close is the highest close of the past x bars.
            Sell if current bar's close is the lowest close of the past x bars.
        Exit:
            Exit long position if close is the lowest close of the past y bars.
            Exit short position if close is the highest close of the past y bars.

        Parameters
        ----------
        x : TYPE
            Channel length (for entry).
        y : TYPE
            Channel length (for exit).

        Returns
        -------
        None.

        '''
        
        self.position = 0 # initial netural position
        self.trades = 0 # no of trades yet
        self.amount = self.initial_amount # reset initial capital
        self.results =[] # reset results dictionary
        self.order_history = [] # reset order_history
        
        # Calculate required indicators:
        self.data['return'] = self.data['Close']/ self.data['Close'].shift(1) - 1
        self.data['xMax'] = self.data['Close'].rolling(x).max().shift(1)
        self.data['xMin'] = self.data['Close'].rolling(x).min().shift(1)
        self.data['yMax'] = self.data['Close'].rolling(x).max().shift(1)
        self.data['yMin'] = self.data['Close'].rolling(x).min().shift(1)
        
        # Trading period:
        start_bar = self.data.index.get_loc(self.start)
        end_bar = self.data.index.get_loc(self.end)
        
        # Run Strategy:
        for bar in range(start_bar, end_bar + 1):
            
            # Enable stop loss and take profit orders
            if self.enable_stop_orders == True:
                self.check_stop_loss(bar=bar)
                self.check_take_profit(bar=bar)
            # Check for Long entry signal
            if self.position in [0, -1]:
                if (self.data['Close'].iloc[bar] > self.data['xMax'].iloc[bar] 
                    and self.data['Close'].iloc[bar-1] <= self.data['xMax'].iloc[bar-1]
                    ) :
                    self.go_long(bar, amount='all', sl=self.sl, tp=self.tp)
                    self.position = 1 # long position
                    print('-' * 55) 
            # Check for Long exit signal
            if self.position == 1:
                if (self.data['Close'].iloc[bar] < self.data['yMin'].iloc[bar]
                    and self.data['Close'].iloc[bar-1] >= self.data['yMin'].iloc[bar-1]
                    ):
                    self.place_sell_order(bar=bar,units=self.units)
                    self.position = 0 # neutral position
                    print('-' * 55)
                    
            # Check for Short entry signal
            if self.position in [0, 1]:
                if (self.data['Close'].iloc[bar] < self.data['xMin'].iloc[bar]
                    and self.data['Close'].iloc[bar-1] >= self.data['xMin'].iloc[bar-1]
                    ):
                    self.go_short(bar,amount='all', sl=self.sl, tp=self.tp)
                    self.position = -1 # short position
                    print('-' * 55)
            # Check for Short exit signal
            if self.position in [0, 1]:
                if (self.data['Close'].iloc[bar] > self.data['yMax'].iloc[bar]
                    and self.data['Close'].iloc[bar-1] <= self.data['yMax'].iloc[bar-1]
                    ):
                    self.place_buy_order(bar=bar,units=-self.units)
                    self.position = 0 # neutral position
                    print('-' * 55)
            
            self.update_results(bar)
            
        self.close_out(bar)
        
    def run_vol_breakout_strategy(self, n=14 ,m=1):
        '''
        

        Parameters
        ----------
        n : TYPE, optional
            DESCRIPTION. The default is 14.
        m : TYPE, optional
            DESCRIPTION. The default is 1.

        Returns
        -------
        None.

        '''
        
        self.position = 0 # initial netural position
        self.trades = 0 # no of trades yet
        self.amount = self.initial_amount # reset initial capital
        self.results = [] # reset results dictionary
        self.order_history = [] # reset order_history
        
        data = self.data.copy()
        
        # Indicators
        data['return'] = data['Close']/ data['Close'].shift(1) - 1
        data['ATR'] = ta.ATR(data,n=14)
        data['Upper_trigger'] = data['Close'].shift() + m * data['ATR']
        data['Lower_trigger'] = data['Close'].shift() - m * data['ATR']
        
        # Trading period:
        start_bar = self.data.index.get_loc(self.start)
        end_bar = self.data.index.get_loc(self.end)
        
        for bar in range(start_bar, end_bar + 1):
            
            # Enable stop loss and take profit orders
            if self.enable_stop_orders == True:
                self.check_stop_loss(bar=bar)
                self.check_take_profit(bar=bar)
            # Check for Long entry signal
            if self.position in [0, -1]:
                if (data['Close'].iloc[bar] > data['Upper_trigger'].iloc[bar] 
                    and data['Close'].iloc[bar-1] <= data['Upper_trigger'].iloc[bar-1]
                    ):
                    self.go_long(bar, amount='all', sl=self.sl, tp=self.tp)
                    self.position = 1 # long position
                    print('-' * 55) 
            # Check for Short entry signal
            if self.position in [0, 1]:
                if (data['Close'].iloc[bar] < data['Lower_trigger'].iloc[bar]
                    and data['Close'].iloc[bar-1] >= data['Lower_trigger'].iloc[bar-1]
                    ):
                    self.go_short(bar,amount='all', sl=self.sl, tp=self.tp)
                    self.position = -1 # short position
                    print('-' * 55)
            
            self.update_results(bar)
            
            #self.update_trailling_sl(bar)

        self.close_out(bar)
        
    def run_buy_and_hold(self):
        '''
        
        Returns
        -------
        None.

        '''
        
        # Reset:
        self.position = 0 # initial netural position
        self.trades = 0 # no of trades yet
        self.amount = self.initial_amount # reset initial capital
        self.results =[] # reset results dictionary
        self.order_history = [] # reset order_history
        
        # Trading period:
        start_bar = self.data.index.get_loc(self.start)
        end_bar = self.data.index.get_loc(self.end)
        
        # Run Strategy:
        for bar in range(start_bar, end_bar + 1):
            
            # Check for Long entry signal
            if self.position in [0, -1]:
                self.go_long(bar, amount='all', sl=self.sl, tp=self.tp)
                self.position = 1 # long position
                if self.verbose:
                    print('-' * 55) 
 
            self.update_results(bar)
            
        self.close_out(bar)
        
#%% Test

if __name__ == '__main__':   
    lsbt = BacktestLongShort(exchange='bybit',
                             symbol='WIFUSDT',
                             interval=60,
                             start='2024-04-01 00:00:00',
                             end='2024-04-30 23:00:00',
                             amount=10000,
                             ptc=0.002,
                             enable_stop_orders=False,
                             enable_filter=False,
                             sl=0.04,
                             tp=None)
    
    lsbt.run_sma_strategy(50, 200)
    #lsbt.run_buy_and_hold()
    fig, axs = plt.subplots(2, gridspec_kw={'height_ratios': [2, 1]}, sharex=True)
    lsbt.plot_equity(ax=axs[0])
    lsbt.plot_drawdowns(ax=axs[1])
    
    #lsbt.run_vol_breakout_strategy()
