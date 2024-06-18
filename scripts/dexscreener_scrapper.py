import asyncio
import websockets
import json
import time
from datetime import datetime
import requests
import csv
import aiofiles
import schedule


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

def job(token_address):
    
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_address}'
    
    repsonse = requests.get(url)
    
    data = repsonse.json()
    pairs = data["pairs"]
    
    # PAIR DATA
    for pair in pairs:
        
        timestamp = int(time.time())

        chain_id = pair["chainId"]
        dex_id = pair["dexId"]
        pair_address = pair["pairAddress"]
       
        token_address = pair["baseToken"]["address"]
    
        token_name = pair["baseToken"]["name"]
        
        #TODO: Remove special characters in the token name.
        token_symbol = pair["baseToken"]["symbol"]
       
        token_txns = pair["txns"]
       
        token_m5_buys = token_txns["m5"]["buys"]
        token_m5_sells = token_txns["m5"]["sells"]
       
        token_h1_buys = token_txns["h1"]["buys"]
        token_h1_sells = token_txns["h1"]["sells"]
       
        token_h1_to_m5_buys = round(token_m5_buys*12 / token_h1_buys, 2) if token_m5_buys else None
       
        token_liquidity = pair.get("liquidity", {}).get("usd")
        token_market_cap = pair.get("marketCap")
       
        token_created_at_raw = pair.get("pairCreatedAt")
        token_created_at = token_created_at_raw / 1000 if token_created_at_raw else None
        token_created_at = datetime.utcfromtimestamp(token_created_at) if token_created_at else None
        if token_created_at:
            now_utc = datetime.utcnow()
            token_created_since = round((now_utc - token_created_at).total_seconds() /60, 2) # minutes since token created.
        else:
            token_created_since = None
           
        token_eti = pair.get("ear", False) # What is ear?
        token_header = pair.get("profile", {}).get("header", False) # What is the header
        token_website = pair.get("profile", {}).get("website", False)
        token_twitter = pair.get("profile", {}).get("twitter", False)
        token_links = pair.get("profile", {}).get("linkCount", False)
        token_img_key = pair.get("profit", {}).get("imgKey", False)
       
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
          token_h1_to_m5_buys, 
          token_liquidity, 
          token_market_cap, 
          token_created_at, 
          token_created_since, 
          token_eti,
          token_header, 
          token_website,
          token_twitter, 
          token_links, 
          token_img_key,
          token_price_usd,
          token_price_change_h24, 
          token_price_change_h6, 
          token_price_change_h1, 
          token_price_change_m5
            ]
            
        print(f"{token_name} data collected!")
            
        # Save data to text file
        async with aiofiles.open("dexscreener_listings.txt", "a") as f:
            STR_VALUES = [str(value) for value in VALUES]
            await f.write(';'.join(STR_VALUES) + '\n')
    
    

async def dexscreener_scraper():
    headers = {
        'Pragma': 'no-cache',
        'Origin': 'https://dexscreener.com',
        'Accept-Language': 'en-US,en;q=0.9',
        'Sec-WebSocket-Key': 'd87c5XMvQA1y1Eet867HFQ==',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Upgrade': 'websocket',
        'Cache-Control': 'no-cache',
        'Connection': 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
    }
    
    url ="wss://io.dexscreener.com/dex/screener/pairs/m5/1?rankBy[key]=pairAge&rankBy[order]=asc&filters[chainIds][0]=solana"
    
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
            print(f"An error occured: {e}")
            await asyncio.sleep(5)

prev_token_address = None
async def handle_message(msg):
    global prev_token_address
    
    message = json.loads(msg) # converts a json into a dictionary...
    
    if isinstance(message, str):
        return
    
    if message['type'] == "pairs":
        pairs = message["pairs"]
        pair = pairs[0]
        
        timestamp = int(time.time())
        # PAIR DATA
        chain_id = pair["chainId"]
        dex_id = pair["dexId"]
        pair_address = pair["pairAddress"]
       
        assert pair_address
       
        token_address = pair["baseToken"]["address"]
        
        if prev_token_address == token_address:
            pass
        else:
            token_name = pair["baseToken"]["name"]
            
            #TODO: Remove special characters in the token name.
            token_symbol = pair["baseToken"]["symbol"]
           
            token_txns = pair["txns"]
           
            token_m5_buys = token_txns["m5"]["buys"]
            token_m5_sells = token_txns["m5"]["sells"]
           
            token_h1_buys = token_txns["h1"]["buys"]
            token_h1_sells = token_txns["h1"]["sells"]
           
            token_h1_to_m5_buys = round(token_m5_buys*12 / token_h1_buys, 2) if token_m5_buys else None
           
            token_liquidity = pair.get("liquidity", {}).get("usd")
            token_market_cap = pair.get("marketCap")
           
            token_created_at_raw = pair.get("pairCreatedAt")
            token_created_at = token_created_at_raw / 1000 if token_created_at_raw else None
            token_created_at = datetime.utcfromtimestamp(token_created_at) if token_created_at else None
            if token_created_at:
                now_utc = datetime.utcnow()
                token_created_since = round((now_utc - token_created_at).total_seconds() /60, 2) # minutes since token created.
            else:
                token_created_since = None
               
            token_eti = pair.get("ear", False) # What is ear?
            token_header = pair.get("profile", {}).get("header", False) # What is the header
            token_website = pair.get("profile", {}).get("website", False)
            token_twitter = pair.get("profile", {}).get("twitter", False)
            token_links = pair.get("profile", {}).get("linkCount", False)
            token_img_key = pair.get("profit", {}).get("imgKey", False)
           
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
              token_h1_to_m5_buys, 
              token_liquidity, 
              token_market_cap, 
              token_created_at, 
              token_created_since, 
              token_eti,
              token_header, 
              token_website,
              token_twitter, 
              token_links, 
              token_img_key,
              token_price_usd,
              token_price_change_h24, 
              token_price_change_h6, 
              token_price_change_h1, 
              token_price_change_m5
                ]
            
            print(f"{token_name} created at {token_created_at}")
            
            # Save data to text file
            async with aiofiles.open("dexscreener_listings.txt", "a") as f:
                STR_VALUES = [str(value) for value in VALUES]
                await f.write(';'.join(STR_VALUES) + '\n')
                
            prev_token_address = token_address
            
            # Schedule the collection of future data
            schedule.every(5).minutes.do(lambda: job(token_address))
            print(f'Data collection for {token_name} has been scheduled...')

if __name__ == '__main__':
    asyncio.run(dexscreener_scraper())