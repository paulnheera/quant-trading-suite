

#%% Libraries

import asyncio # How we interact with websockets using the websockets library.
from websockets import connect
from websockets.client import connect as websocket_connect
import aiofiles # allows us to save to a file asynchronously.
import sys
import json
import httpx # similar to the request library, but allows us to make requests asynchronously.
from datetime import datetime
from configparser import ConfigParser
from websockets.exceptions import ConnectionClosedError

#%% Config
config = ConfigParser()
config.read('algo_trading.cfg')
api_key = config.get('luno', 'api_key')
api_secret = config.get('luno', 'api_secret')

#%% 
sequence = None
bids = None
asks = None

async def connect(pair):
    global sequence
    global bids
    global asks
    
    # What does this function do?
    # Connects to the Websocket and retrieve the first message.
    
    pair = pair.upper()
    url = f"wss://ws.luno.com/api/1/stream/{pair}"
    
    #async with connect(url) as websocket: # subscribing to the websocket
    
    print(f'Connecting to {url}...')
    websocket = await websocket_connect(url, max_size=2**21) # create websocket connection object.
         
    # The client must start by sending API key credentials
    await websocket.send(json.dumps({
        'api_key_id': api_key,
        'api_key_secret': api_secret
        }))
    
    initial = await websocket.recv()
    initial_data = json.loads(initial)
    sequence = int(initial_data['sequence'])
    
    asks = {x['id']: [float(x['price']), float(x['volume'])] for x in initial_data['asks']}
    bids = {x['id']: [float(x['price']), float(x['volume'])] for x in initial_data['bids']}
    print('Initial state recieved.')
    
    # Save the message data
    today = datetime.now().date()
    async with aiofiles.open(f'XBTZAR - OB snapshot - {today}.txt', mode='a') as f:
        await f.write(initial + "\n")
    
    return websocket
    
    #data = await websocket.recv() # recieves inital data
    
async def handle_message(message): # coroutine function
    # print the message received from the websocket connection
    #print(message)
    # Save the message data
    today = datetime.now().date()
    async with aiofiles.open(f'XBTZAR - OB updates - {today}.txt', mode='a') as f:
        await f.write(message + "\n")

async def handle_message2(message): # coroutine function
    global sequence

    data = json.loads(message)
    new_sequence = int(data['sequence'])
    
    if new_sequence != sequence + 1:
        print(f'Sequence broken: expected "{sequence+1}", received "new_sequence".')
        
    sequence = new_sequence
    
    trades = process_message(data)
    
    today = datetime.now().date()
    # Save trades
    if trades:
        async with aiofiles.open(f'XBTZAR - OB updates - {today}.txt', mode='a') as f:
            await f.write(str(trades) + "\n")
    
    # Consolidate order book and save snapshot
    
    
    # Save the message data
    # async with aiofiles.open(f'XBTZAR - OB updates - {today}.txt', mode='a') as f:
    #     await f.write(message + "\n")

def process_message(data):
    
    global asks
    global bids
    
    if data['delete_update']:
        order_id = data['delete_update']['order_id']
        
        try:
            del bids[order_id]
        except KeyError:
            pass
        try:
            del asks[order_id]
        except KeyError:
            pass
    
    if data['create_update']:
        update = data['create_update']
        price = float(update['price'])
        volume = float(update['volume'])
        key = update['order_id']
        book = bids if update['type'] == 'BID' else asks
        book[key] = [price, volume]
        
    trades = []
    
    if data['trade_updates']:
        for update in data['trade_updates']:
            update['price'] = float(update['counter']) / float(update['base'])
            market_order_id = update['maker_order_id']
            if market_order_id in bids:
                # update existing order
                update_existing_order(bids, update=update)
                trades.append({**update, 'type':'sell'})
            elif market_order_id in asks:
                # update existing order
                update_existing_order(asks, update=update)
                trades.append({**update, 'type':'buy'})
                
    return trades

def update_existing_order(book, update):
    order_id = update['maker_order_id']
    existing_order = book[order_id]
    existing_volume = existing_order[1]
    new_volume = existing_volume - float(update['base']) # base is the amount in the base asset e.g. BTC in a BTCZAR pair.
    if new_volume == float('0'):
        del book[order_id]
    else:
        existing_order[1] -= float(update['base'])
    

    
async def run(pair):
    while True:
        try:
            websocket = await connect(pair) # create a websocket connection object, with authentication.
            async for message in websocket: # Loop for incoming messages. # This is asynchronous and non-blocking.
                if message == '""':
                    # Keep alive
                    continue
                await handle_message2(message)
        except ConnectionClosedError:
            print("Connection closed unexpectedly. Reconnecting...")
            await asyncio.sleep(5)  # Wait for a few seconds before attempting to reconnect

# When there is an error with the websocket, do I have to call the run function again?
  
#await run('XBTZAR') 

async def main():       
    await run('XBTZAR') # asyncio.run(orderbook_download('XBTZAR'))
    
asyncio.run(main())



## TODO: Keep stream alive.

#import luno_streams

#TODO:
# Implement Error Handling: Implement error handling in the code to gracefully handle disconnections
# and errors. When the connection is unexpectedly closed, you can catch the `ConnectionClosedError` exception
# and attempt to reconnect.

#NOTES:
# "Both the client and server must send regular **keep alive** messages to avoid disconnection during periods
# of lowe update message activity"

# "If there is any error while processing an update or there is a network error or timeout, the client should
# close the connection and reconnect in order to reinitialise its state.

# Huh?? - It is important that client implement some kind of **backoff** to avoid being rate limited in case of errors.

#ERRORS RECEIVED:
# websockets.exceptions.ConnectionClosedError: no close frame received or sent
#  - "Typically occurs when the WebSocket connection is closed unexpectedly without sending or receiving a close frame."

# websockets.exceptions.InvalidStatusCode: server rejected WebSocket connection: HTTP 502
