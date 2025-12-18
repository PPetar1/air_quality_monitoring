#!/usr/bin/env python3
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
from openaq import AsyncOpenAQ

API_KEY = "28167e861b5b17b41ef6f9e4ab183790ae37b8df75697bbb95212edde98e215d"
COUNTRIES = [132, 110, 103, 80, 75, 65, 131, 62, 74, 97, 104] #ids of all the balkan countries

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

async def fetch_and_save_data(output_dir, func, *args, **kwargs):
    results = True
    page_num = 0

    kwargs['limit'] = 1000
    
    while results:
        page_num += 1
        kwargs['page'] = page_num
        response1 = func(*args, **kwargs)

        page_num += 1
        kwargs['page'] = page_num
        response2 = func(*args, **kwargs)

        page_num += 1
        kwargs['page'] = page_num
        response3 = func(*args, **kwargs)

        page_num += 1
        kwargs['page'] = page_num
        response4 = func(*args, **kwargs)

        page_num += 1
        kwargs['page'] = page_num
        response5 = func(*args, **kwargs)

        responses = await asyncio.gather(response1, response2, response3, response4, response5)

        if len(responses[0].results) == 0:
            results = False
        else:
            data = responses[0].dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)

        if len(responses[1].results) == 0:
            results = False
        else:
            data = responses[1].dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)

        if len(responses[2].results) == 0:
            results = False
        else:
            data = responses[2].dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)

        if len(responses[3].results) == 0:
            results = False
        else:
            data = responses[3].dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)

        if len(responses[4].results) == 0:
            results = False
        else:
            data = responses[4].dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)

async def main():
    client = AsyncOpenAQ(api_key=API_KEY)
    save_path = "data/raw/"
    await fetch_and_save_data(save_path + "locations", client.locations.list, countries_id = COUNTRIES)
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
