import pandas as pd
import aiohttp
import asyncio
import aiofiles
import json
import schedule
import threading
import time
from datetime import datetime, timedelta

seen_tokens = set()  # Set to track seen token addresses

async def fetch_latest_profiles():
    """Fetch the latest token profiles from the API."""
    url = "https://api.dexscreener.com/token-profiles/latest/v1"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Failed to fetch data. Status code: {response.status}")
                return []

async def fetch_token_data(token_address):
    """Fetch data for a specific token address."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Failed to fetch data for {token_address}. Status code: {response.status}")
                return None

def transform_data(pair, timestamp):
    """Transform the API response into a standard structure."""
    chain_id = pair["chainId"]  # solana
    dex_id = pair["dexId"]  # raydium
    pair_address = pair["pairAddress"]  # x0XX

    token_address = pair["baseToken"]["address"]
    token_symbol = pair["baseToken"]["symbol"]

    # Transactions
    token_txns = pair.get("txns", {})
    token_m5_buys = token_txns.get("m5", {}).get("buys", 0)
    token_m5_sells = token_txns.get("m5", {}).get("sells", 0)
    token_h1_buys = token_txns.get("h1", {}).get("buys", 0)
    token_h1_sells = token_txns.get("h1", {}).get("sells", 0)
    token_h6_buys = token_txns.get("h6", {}).get("buys", 0)
    token_h6_sells = token_txns.get("h6", {}).get("sells", 0)
    token_h24_buys = token_txns.get("h24", {}).get("buys", 0)
    token_h24_sells = token_txns.get("h24", {}).get("sells", 0)

    # Market Information
    token_liquidity = pair.get("liquidity", {}).get("usd", 0)
    token_fdv = pair.get("fdv", 0)  # Fully Diluted Value

    # Price Change Info
    token_price_change = pair.get("priceChange", {})
    token_price_change_m5 = token_price_change.get("m5", 0)
    token_price_change_h1 = token_price_change.get("h1", 0)
    token_price_change_h6 = token_price_change.get("h6", 0)
    token_price_change_h24 = token_price_change.get("h24", 0)

    # Price
    token_price_native = pair.get("priceNative", 0)
    token_price_usd = pair.get("priceUsd", 0)

    # Other Information
    token_created_at = pair.get("pairCreatedAt", 0)  # Timestamp in milliseconds

    # Place into and return a dictionary
    data = {
        "timestamp": timestamp,
        "token_created_at": token_created_at,
        "chain_id": chain_id,
        "dex_id": dex_id,
        "pair_address": pair_address,
        "token_address": token_address,
        "token_symbol": token_symbol,
        "token_m5_buys": token_m5_buys,
        "token_m5_sells": token_m5_sells,
        "token_h1_buys": token_h1_buys,
        "token_h1_sells": token_h1_sells,
        "token_h6_buys": token_h6_buys,
        "token_h6_sells": token_h6_sells,
        "token_h24_buys": token_h24_buys,
        "token_h24_sells": token_h24_sells,
        "token_liquidity": token_liquidity,
        "token_fdv": token_fdv,
        "token_price_change_m5": token_price_change_m5,
        "token_price_change_h1": token_price_change_h1,
        "token_price_change_h6": token_price_change_h6,
        "token_price_change_h24": token_price_change_h24,
        "token_price_native": token_price_native,
        "token_price_usd": token_price_usd,
    }

    return data

write_queue = asyncio.Queue() # Central queue for storing data to write
async def append_to_file(filename="dexscreener_data.txt"):
    """Append transformed data to a text file."""

    while True:
        data = await write_queue.get() # Get data from the queue
        today = datetime.now().date()
        async with aiofiles.open(f"dexscreener_data - {today}.txt", mode="a", encoding="utf-8") as file:
            line = json.dumps(data)  # Convert dictionary to JSON string
            await file.write(line + "\n")
            #print(f"Data written to file: {data}")  # Debugging statement
            dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{dt} | Data for {data.get('pair_address')} written to file.")
            write_queue.task_done()

async def process_token(token_address):
    """Fetch, transform, and store data for a token."""
    timestamp = int(time.time())  # Current timestamp in seconds
    dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{dt} | Fetching data for token: {token_address}")
    data = await fetch_token_data(token_address)
    if data:
        transformed_data = [transform_data(pair, timestamp) for pair in data.get("pairs", [])]
        for d in transformed_data:
            await write_queue.put(d) # Add the data to the queue
        dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{dt} | Data for token {token_address} queued for storage.")

def schedule_token_task(loop, token_address): #TODO: Change this function name to just token_task
    """Schedule an async task to fetch and store token data."""
    #print(f"Scheduling task for token: {token_address}")  # Debugging statement
    asyncio.run_coroutine_threadsafe(process_token(token_address), loop)

def run_scheduler():
    """Run the scheduler in a separate thread."""
    while True:
        schedule.run_pending()
        time.sleep(1)

async def schedule_task_with_delay(loop, token_address, delay=300):
    """Encapsulates scheduling a task with a delay."""
    
    # Get token data and select the create time of the first pair
    res = await fetch_token_data(token_address)
    create_time = res['pairs'][0]['pairCreatedAt']
    
    # Calculate delay
    start_dt = pd.to_datetime(create_time,unit='ms')
    dt_now = datetime.utcnow()
    while start_dt < dt_now:
        start_dt += timedelta(minutes=5)
        
    delay = (start_dt - dt_now).total_seconds() # May need to leave a little room for slippage of time.
    
    await asyncio.sleep(delay)  # Delay in seconds
    schedule.every(5).minutes.do(schedule_token_task, loop, token_address)
    dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{dt} | Task scheduled for token: {token_address}")
    
async def process_new_profiles(profiles):
    """Process the list of token profiles, identifying new ones."""
    global seen_tokens
    for profile in profiles:
        token_address = profile["tokenAddress"]
        if token_address not in seen_tokens:
            seen_tokens.add(token_address)
            dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{dt} | New token found: {token_address}")
            
            # Schedule the task asynchronously with delay
            asyncio.create_task(schedule_task_with_delay(asyncio.get_event_loop(), token_address))
            #schedule.every(5).minutes.do(schedule_token_task, asyncio.get_event_loop(), token_address)

async def main():
    dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{dt} | Process started...')
    print('')
    
    # Start the file-writing task
    asyncio.create_task(append_to_file())
    
    # Start the scheduler in a separate thread
    threading.Thread(target=run_scheduler, daemon=True).start()

    while True:
        profiles = await fetch_latest_profiles()
        await process_new_profiles(profiles)
        await asyncio.sleep(10)  # Poll every 10 seconds

asyncio.run(main())


"""
To Do:
======


Comments:
=========


"""
