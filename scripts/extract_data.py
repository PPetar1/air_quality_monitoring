#!/usr/bin/env python3

import asyncio
from openaq import AsyncOpenAQ
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import os
import logging
import sys

logging.getLogger("transport").setLevel(logging.CRITICAL)

SCRIPT_NAME = os.path.basename(__file__)
API_KEY = "28167e861b5b17b41ef6f9e4ab183790ae37b8df75697bbb95212edde98e215d"
COUNTRY_IDS = [132, 110, 103, 80, 75, 65, 131, 62, 74, 97, 104] #ids of all the balkan countries
BATCH_SIZE = 5
LIMIT = 1000
MAX_RETRY = 3
API_LIMIT_PER_MINUTE = 60
SAVE_PATH = "data/raw/"
LOCATION_IDS = []
SENSOR_IDS = []
DATETIME_FROM = datetime.now() - timedelta(days=1)

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

async def fetch_data_and_save(output_dir, async_func, *args, **kwargs):
    await RATE_LIMITER.acquire()
    
    try:
        response = await execute_with_retry(async_func, *args, **kwargs) 
    except Exception as err:
        sys.stderr.write("A task has failed " + str(MAX_RETRY + 1) + " times. Error message: " + str(err) + "\n")
        return

    RATE_LIMITER.release()

    data = response.dict()
    df = pd.json_normalize(data['results'])

    if 'locations_id' in kwargs:
        df['location_id'] = kwargs['locations_id']

    
    save_data(df, output_dir)


async def fetch_paged_data_and_save(output_dir, async_func, batch_size=1, *args, **kwargs):
    results = True
    page_num = 0

    while results:
        await RATE_LIMITER.acquire(batch_size)
        
        tasks = []
        for _ in range(batch_size):
            page_num += 1
            kwargs['page'] = page_num

            tasks.append(execute_with_retry(async_func, *args, **kwargs))
        
        error_count = 0

        async for task in asyncio.as_completed(tasks):
            try:
                response = task.result() 
            except Exception as err:
                sys.stderr.write("A task has failed " + str(MAX_RETRY + 1) + " times. Error message: " + str(err) + "\n")
                error_count += 1
                continue
                
            RATE_LIMITER.release()

            if len(response.results) == 0:
                results = False
            else:
                data = response.dict()
                df = pd.json_normalize(data['results'])
            
                if 'sensors_id' in kwargs:
                    df['sensor_id'] = kwargs['sensors_id']
            
                save_data(df, output_dir)

        if error_count == batch_size:
            break

async def execute_with_retry(async_func, max_retry=MAX_RETRY, *args, **kwargs):
    retry_count = 0
    while True:
        try:
            if retry_count != 0:
                await RATE_LIMITER.acquire()
            return await async_func(*args, **kwargs)
        except Exception as err:
            RATE_LIMITER.release()
            if retry_count < max_retry:
                retry_count += 1
            else:
                raise err
   
async def fetch_and_save_ids(api_call, id_list, *args, **kwargs):
    response = await api_call(*args, **kwargs)
    id_list.extend(map(lambda x: x.id, response.results))
    return response

async def main():
    try:
        start_time = datetime.now()
        print("Script " + SCRIPT_NAME + " starting execution at: " + start_time.strftime("%H:%M:%S"))
        
        client = AsyncOpenAQ(api_key=API_KEY)
        
        tasks = []
        tasks.append(fetch_paged_data_and_save(SAVE_PATH + "location", fetch_and_save_ids, api_call=client.locations.list, id_list=LOCATION_IDS, batch_size=BATCH_SIZE, countries_id=COUNTRY_IDS))
        tasks.append(fetch_paged_data_and_save(SAVE_PATH + "country", client.countries.list, batch_size=BATCH_SIZE))
        tasks.append(fetch_paged_data_and_save(SAVE_PATH + "parameters", client.parameters.list, batch_size=BATCH_SIZE))
     
        await asyncio.gather(*tasks)


        tasks = []
        for locations_id in LOCATION_IDS:
            tasks.append(fetch_data_and_save(SAVE_PATH + "sensor", fetch_and_save_ids, api_call=client.locations.sensors, id_list=SENSOR_IDS, locations_id=locations_id))

        await asyncio.gather(*tasks)

        tasks = []
        for sensors_id in SENSOR_IDS:
            tasks.append(fetch_paged_data_and_save(SAVE_PATH + "measurement", client.measurements.list, sensors_id=sensors_id, datetime_from=DATETIME_FROM))

        await asyncio.gather(*tasks)


        await client.close()

        end_time = datetime.now()
        print("Script " + SCRIPT_NAME + " finished execution at: " + end_time.strftime("%H:%M:%S") + "\nExecution time: " + str((end_time - start_time).total_seconds()) + "s")
    
    except Exception as err:
        end_time = datetime.now()
        sys.stderr.write("CRITICAL\nScript " + SCRIPT_NAME + " encountered an unrecoverable error at: " + end_time.strftime("%H:%M:%S") +  
              "\nError message: ||| " + str(err) + " |||\nExecution time: " + str((end_time - start_time).total_seconds()) + "s")

if __name__ == "__main__":
    asyncio.run(main())

# TODO: 
#       logging,
#       documentation
