# Securities master setup / initialization

import pandas as pd
import time
from datetime import datetime
import sqlite3
from binance.client import Client



def create_klines_table(interval='1H'):

    # create a sql connection
    con = sqlite3.connect('data/securities_master.db')
    c = con.cursor()
    
    table_name = f'KLINES_{interval}'
    
    # If table exists print table already exists and exit function
    c.execute(f""" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='KLINES_{interval}' """)
    if c.fetchone()[0]==1: 
        print('Table already exists!')
        return
        
    # Create price table for the relevant time interval
    query = f"""CREATE TABLE IF NOT EXISTS {table_name} 
    (
    Exchange TEXT NOT NULL,
    Product_Type TEXT NOT NULL,
    Symbol TEXT NOT NULL,
    Time DATETIME NOT NULL,
    Open DOUBLE NOT NULL,
    High DOUBLE NOT NULL,
    Low DOUBLE NOT NULL,
    Close DOUBLE NOT NULL,
    Volume DOUBLE NOT NULL,
    Load_time DATETIME NOT NULL
    )
    """
    c.execute(query.replace('\n', ' '))
    
    # Check the table created:
    c.execute(""" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='KLINES_1H' """)
    
    # If the count is 1, then table exists
    if c.fetchone()[0]==1 : {
    	print('Table created.')
    }
    
    # Close the connection
    con.close()
    
if __name__ == '__main__':
    
    create_klines_table(interval='1D')


# Create Asset Info table

# create a sql connection
con = sqlite3.connect('data/securities_master.db')
c = con.cursor()

query = """CREATE TABLE IF NOT EXISTS  ASSET_INFO 
(
Exchange TEXT NOT NULL,
Product_Type TEXT NOT NULL,
Symbol TEXT NOT NULL,
Status TEXT NOT NULL,
Launch_Time DATETIME,
Delivery_Time DATETIME,
Base_Asset TEXT NOT NULL,
Quote_Asset TEXT NOT NULL,
Min_Price DOUBLE NOT NULL,
Max_Price DOUBLE NOT NULL,
Tick_Size DOUBLE NOT NULL,
Max_Qty DOUBLE NOT NULL,
Min_Qty DOUBLE NOT NULL,
Qty_Step DOUBLE NOT NULL,
Load_Time DATETIME NOT NULL
)
"""
c.execute(query.replace('\n', ' '))

# Check the table created:
c.execute(""" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='ASSET_INFO' """)

# If the count is 1, then table exists
if c.fetchone()[0]==1 : {
	print('Table exists.')
}

# Close the connection
con.close()


    
