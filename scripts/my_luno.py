

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

#%% Config
config = ConfigParser()
config.read('algo_trading.cfg')
api_key = config.get('luno', 'api_key')
api_secret = config.get('luno', 'api_secret')

#%% 

async def connect(pair):
    # What does this function do?
    
    pair = pair.upper()
    url = f"wss://ws.luno.com/api/1/stream/{pair}"
    
    #async with connect(url) as websocket: # subscribing to the websocket
    
    websocket = await websocket_connect(url, max_size=2**21) # create websocket connection object.
         
    await websocket.send(json.dumps({
        'api_key_id': api_key,
        'api_key_secret': api_secret
        }))
    
    initial = await websocket.recv()
    print(initial)
    # Save the message data
    today = datetime.now().date()
    async with aiofiles.open(f'XBTZAR - OB snapshot - {today}.txt', mode='a') as f:
        await f.write(initial + "\n")
    
    return websocket
    
    #data = await websocket.recv() # recieves inital data
    
async def handle_message(message): # coroutine function
    # print the message received from the websocket connection
    print(message)
    # Save the message data
    today = datetime.now().date()
    async with aiofiles.open(f'XBTZAR - OB updates - {today}.txt', mode='a') as f:
        await f.write(message + "\n")

    
async def run(pair):
    websocket = await connect(pair) # create a websocket connection object, with authentication.
    async for message in websocket: # Loop for incoming messages.
        if message == '""':
            continue
        await handle_message(message)
   
       
await run('XBTZAR') # asyncio.run(orderbook_download('XBTZAR'))

#import luno_streams
