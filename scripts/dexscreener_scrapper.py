import pandas as pd
import asyncio
import websockets
import json
import time
from datetime import datetime
import requests
import aiofiles
import schedule
import threading

DATA = []
FIELDNAMES = [
    "chain_id", 
    "dex_id", 
    "pair_address", 
    "token_address", 
    "token_name", 
    "token_symbol", 
    "token_m5_buys", 
    "token_m5_sells", 
    "token_h1_buys", 
    "token_h1_sells", 
    "token_h1_to_m5_buys", 
    "token_liquidity", 
    "token_market_cap", 
    "token_created_at", 
    "token_created_since", 
    "token_eti", 
    "token_header", 
    "token_website", 
    "token_twitter", 
    "token_links", 
    "token_img_key", 
    "token_price_usd", 
    "token_price_change_h24", 
    "token_price_change_h6",
    "token_price_change_h1", 
    "token_price_change_m5"
]

request_counters = {} # Dictionary to track request counts per token

async def get_latest(token_address):
    """
    Collects data at a point in time from the dexscreener api.
    """
    
    global request_counters
    
    # Increment the counter for this token
    request_counters[token_address] = request_counters.get(token_address, 0) + 1
    
    if request_counters[token_address] > 48:
        # Stop the job for this token after 48 requests
        print(f"Stopping data collection for {token_address} after 48 requests.")
        return schedule.CancelJob
    
    url = f'https://api.dexscreener.io/latest/dex/tokens/{token_address}'
    
    response = requests.get(url)
    data = response.json()
    pairs = data["pairs"]
    
    # PAIR DATA
    for pair in pairs:
        timestamp = int(time.time())

        chain_id = pair["chainId"] # solana
        dex_id = pair["dexId"] # raydium 
        pair_address = pair["pairAddress"] # x0XX
       
        token_address = pair["baseToken"]["address"]
        token_name = pair["baseToken"]["name"]
        token_name = token_name.encode('ascii','ignore').decode('ascii') # Removes non-ASCII charaters
        token_symbol = pair["baseToken"]["symbol"]
        
        # Token Transactions
        token_txns = pair["txns"]
       
        token_m5_buys = token_txns["m5"].get("buys", 0)
        token_m5_sells = token_txns["m5"].get("sells", 0)
       
        token_h1_buys = token_txns["h1"].get("buys", 0)
        token_h1_sells = token_txns["h1"].get("sells", 0)
        
        token_h6_buys = token_txns["h6"].get("buys", 0)
        token_h6_sells = token_txns["h6"].get("sells", 0)
       
        # Other Information
        token_liquidity = pair.get("liquidity", {}).get("usd", 0)
        token_fdv = pair.get("fdv", 0) # Fully Diluted Value
       
        token_created_at = pair.get("pairCreatedAt", 0)
        
        # Price Info
        token_price_native = pair.get("priceNative", 0)
        token_price_usd = pair.get("priceUsd", 0)
        token_price_change_h24 = pair["priceChange"].get("h24", 0)
        token_price_change_h6 = pair["priceChange"].get("h6", 0)
        token_price_change_h1 = pair["priceChange"].get("h1", 0)
        token_price_change_m5 = pair["priceChange"].get("m5", 0)
           
        
        # Encapsulate each value in double quotes and handle empty values as "0" or other defaults
        VALUES = [
            f'"{timestamp}"',
            f'"{chain_id}"', 
            f'"{dex_id}"', 
            f'"{pair_address}"', 
            f'"{token_address}"', 
            #f'"{token_name}"', 
            f'"{token_symbol}"', 
            f'"{token_m5_buys}"', 
            f'"{token_m5_sells}"', 
            f'"{token_h1_buys}"', 
            f'"{token_h1_sells}"',
            f'"{token_h6_buys}"', 
            f'"{token_h6_sells}"',
            f'"{token_liquidity}"', 
            f'"{token_fdv}"',
            f'"{token_created_at}"',
            f'"{token_price_native}"',
            f'"{token_price_usd}"',
            f'"{token_price_change_h24}"', 
            f'"{token_price_change_h6}"', 
            f'"{token_price_change_h1}"', 
            f'"{token_price_change_m5}"'
        ]
        
        
        
        # Save data to text file
        today = datetime.now().date()
        async with aiofiles.open(f"dexscreener_listings - {today}.txt", "a",encoding="utf-8") as f:
            STR_VALUES = [str(value) for value in VALUES]
            await f.write(';'.join(STR_VALUES) + '\n')
            
    print(f"{token_name} data collected!")
    
async def stream_new_tokens():
    """
    Streams the 'New Pairs' page on Dexscreener, recording new listings.

    """
    
    headers = {
        'Upgrade': 'websocket',
        'Origin': 'https://dexscreener.com',
        'Cache-Control': 'no-cache',
        'Accept-Language': 'en-US,en;q=0.9',
        'Pragma': 'no-cache',
        'Connection': 'Upgrade',
        'Sec-WebSocket-Key': 'Hp4FAZ/fjUQ/6fO/qvotoA==',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
    }
    
    url = "wss://io.dexscreener.com/dex/screener/v4/pairs/h24/1?rankBy[key]=pairAge&rankBy[order]=asc"
    
    while True:
        try:
            async with websockets.connect(url, extra_headers=headers) as websocket:
                while True:
                    # Run Scheduled tasks
                    schedule.run_pending()
                    
                    message_raw = await websocket.recv()
                    
                    # Handle Message:
                    await handle_message(message_raw)
                    
        except websockets.ConnectionClosedError:
            print("Connection closed. Reconnecting in 5 seconds...")   
            await asyncio.sleep(5)
        except Exception as e:
            print(f"An error occurred: {e}")
            await asyncio.sleep(5)

prev_token_address = None
async def handle_message(msg):
    global prev_token_address
    
    message = json.loads(msg)  # Convert JSON into a dictionary
    
    if isinstance(message, str):
        return
    
    if message['type'] == "pairs":
        pairs = message["pairs"]
        pair = pairs[0]
        
        # PAIR DATA
        timestamp = int(time.time())

        chain_id = pair["chainId"]
        dex_id = pair["dexId"]
        pair_address = pair["pairAddress"]
       
        token_address = pair["baseToken"]["address"]
        if prev_token_address == token_address:
            return
        #TODO: Remove special characters in the token name or symbol
        token_name = pair["baseToken"]["name"]
        token_name = token_name.encode('ascii','ignore').decode('ascii') # Removes non-ASCII charaters
        token_symbol = pair["baseToken"]["symbol"]
        
        # Token Transactions
        token_txns = pair["txns"]
        
        token_m5_buys = token_txns["m5"]["buys"]
        token_m5_sells = token_txns["m5"]["sells"]
        
        token_h1_buys = token_txns["h1"]["buys"]
        token_h1_sells = token_txns["h1"]["sells"]
        
        token_h6_buys = token_txns["h6"]["buys"]
        token_h6_sells = token_txns["h6"]["sells"]
        
        # Other Info
        token_liquidity = pair.get("liquidity", {}).get("usd")
        token_fdv = pair.get("fdv") # Fully Diluted Value
        
        token_created_at = pair.get("pairCreatedAt")
        
        # Price Info
        # token_price_native = pair["priceNative"]
        token_price_usd = pair["priceUsd"]
        token_price_change_h24 = pair["priceChange"]["h24"]
        token_price_change_h6 = pair["priceChange"]["h6"]
        token_price_change_h1 = pair["priceChange"]["h1"]
        token_price_change_m5 = pair["priceChange"]["m5"]
        
        VALUES = [
            timestamp,
            chain_id, 
            dex_id, 
            pair_address, 
            token_address, 
            token_name, 
            token_symbol, 
            token_m5_buys, 
            token_m5_sells, 
            token_h1_buys, 
            token_h1_sells,
            token_h6_buys, 
            token_h6_sells,
            token_liquidity, 
            token_fdv,
            token_created_at,
            # token_price_native,
            token_price_usd,
            token_price_change_h24, 
            token_price_change_h6, 
            token_price_change_h1, 
            token_price_change_m5
        ]
        
        print(f"New Token: {token_name}, created at {pd.to_datetime(token_created_at,unit='ms')}")
        
        # Save data to text file
        today = datetime.now().date()
        async with aiofiles.open(f"dexscreener_listings - {today}.txt", "a", encoding="utf-8") as f:
            STR_VALUES = [str(value) for value in VALUES]
            await f.write(';'.join(STR_VALUES) + '\n')
            
        # Update token address pointer
        prev_token_address = token_address
        
        # Schedule the collection of future data
        schedule.every(1).hours.do(lambda: asyncio.run_coroutine_threadsafe(get_latest(token_address), asyncio.get_event_loop()))
        print(f'Data collection for {token_name} has been scheduled...')

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # Start the schedule in a separate thread
    threading.Thread(target=run_schedule).start()
    asyncio.run(stream_new_tokens())
    
"""
Get the get_latest job for each symbol/token needs to run for a maximum of 24 hours. i.e. only 24 requests of 
data are made after listing.

- when reading token names and token symbol we should ignore any new line characters.
- maybe encapsulate each value in double quotation marks.
"""

"""
Issues:
1. token names/symbols with new line characters (or something of that sort i.e. big white spaces)
    e.g. "BabyWolf ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀ 3jWrY7tcyUo9tV82h1Rh2irYke4QVaybNhxdLXtipump"
    e.g. "Make Degens Money Again ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀ GmgLZbHUiEU5tUj2KJTRbgP5yqnkmD7xKRCEsS6Kpump"
    
2. there are some lines with 44 columns/values (These need to be identified)
3. Some lines have missing price_change values leading to less columns or joining with the next record.
4. For some reason this pair of wrapped SOL is picked up: FpCMFDFGYotvufJ7HrFHsWEiiQCGbkLCtwHiDnh7o28Q.
5. Some values could be written to the file at the same time as another pair causing the values to be mixed up. Is this a 
possibility???
 -- e.g. C9KGpvkfpnE3e6pqgZePPh6ib1aTBrSVtNqeWMxqpump;CULT
 -- e.g. Et5kbVjcniEn5D8X2gKHzTVKBijx2M92Bg6aCWrX;CATO
 
6. 1723365667;solana;raydium;8Re9oJLHtrXrj6LfG7W2ugmvCygXwSgovSpqkUYhEjm2;9kskv67c4URmBKhpWLsL1UZZjeR8KYm3sRCrh9jBpump;robber dog;robber;1;7;74;103;440;472;43735.79;151712;1723301659000;0.0001517;145;-56.06;-26.48;-16
-- the -16 of the above line was not followed with a new line character and was joint to the next record (for some reason.)
7. Some records/lines are repeated i.e. there are duplicates of the exact same lines.
    -- e.g. 1723365686;solana;fluxbeam;7CJ1gSHur78bnrsqMPDvuEmMMH74Zkok15MYzSYBMEbn;4XjbB8QprYnycsFtHcjbHHkB3ytPGCyAcAouN8BUpump;That's fire;Fire;0;0;0;0;0;0;0.06;62522;1723301249000;0.00006633;-98.64;0;0;0
8. Need to ignore WSOL records that appear as new listings.
"""

