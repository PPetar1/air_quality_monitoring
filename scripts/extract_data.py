#!/usr/bin/env python3

import asyncio
from openaq import AsyncOpenAQ
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path

API_KEY = "28167e861b5b17b41ef6f9e4ab183790ae37b8df75697bbb95212edde98e215d"
COUNTRIES = [132, 110, 103, 80, 75, 65, 131, 62, 74, 97, 104] #ids of all the balkan countries
BATCH_SIZE = 7
API_LIMIT_PER_MINUTE = 60
SAVE_PATH = "data/raw/"

class RateLimiter:
    def __init__(self, max_per_minute=API_LIMIT_PER_MINUTE):
        self.semaphore = asyncio.Semaphore(max_per_minute)
        self.list = [0]*max_per_minute
        self.lock = asyncio.Lock()

    async def acquire(self, amount=1):
        async with self.lock:
            for _ in range(amount):
                await self.semaphore.acquire()
                last_request = self.list.pop(0)

            time_to_wait = last_request + 60 - time.time()
            if time_to_wait > 0:
                await asyncio.sleep(time_to_wait)

    def release(self):
        self.list.append(time.time())
        self.semaphore.release()

RATE_LIMITER = RateLimiter()

def save_data(df, output_dir="data/raw", prefix="", suffix=""):
    if df.empty:
        return None
    
    output_path = Path(output_dir) 
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")
    
    filename = f"{prefix}{timestamp}{suffix}.parquet"
    parquet_file = output_path / filename
    df.to_parquet(parquet_file, index=False)
    
    return parquet_file

async def fetch_and_save_data_batch(output_dir, async_func, batch_size=BATCH_SIZE, *args, **kwargs) :
    results = True
    page_num = 0

    kwargs['limit'] = 1000
    
    while results:
        await RATE_LIMITER.acquire(batch_size)
        
        tasks = []
        for _ in range(batch_size):
            page_num += 1
            kwargs['page'] = page_num

            tasks.append(async_func(*args, **kwargs))
        
        async for task in asyncio.as_completed(tasks):
            response = task.result() 
            RATE_LIMITER.release()

            if len(response.results) == 0:
                results = False
            else:
                data = response.dict()
                df = pd.json_normalize(data['results'])
                save_data(df, output_dir)

async def fetch_and_save_data(output_dir, async_func, *args, **kwargs):
    results = True
    page_num = 0

    kwargs['limit'] = 1000
    
    while results:
        page_num += 1
        kwargs['page'] = page_num

        await RATE_LIMITER.acquire()

        response = await async_func(*args, **kwargs)
        
        RATE_LIMITER.release()
        
        if len(response.results) == 0:
            results = False
        else:
            data = response.dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)
        
async def main():
    client = AsyncOpenAQ(api_key=API_KEY)
    tasks = []
    for _ in range(10):
        tasks.append(fetch_and_save_data_batch(SAVE_PATH + "locations", client.locations.list, countries_id = COUNTRIES))

    for _ in range(5):
        tasks.append(fetch_and_save_data(SAVE_PATH + "locations", client.locations.list, countries_id = COUNTRIES))

    await asyncio.gather(*tasks)

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
