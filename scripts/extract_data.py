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
    page_num = 1

    kwargs['limit'] = 1000
    
    while results:
        kwargs['page'] = page_num
        response = await func(*args, **kwargs)
        
        if len(response.results) == 0:
            results = False
        else:
            data = response.dict()
            df = pd.json_normalize(data['results'])
            save_data(df, output_dir)
            page_num +=1

async def main():
    client = AsyncOpenAQ(api_key=API_KEY)
    save_path = "data/raw/"
    await fetch_and_save_data(save_path + "locations", client.locations.list, countries_id = COUNTRIES)
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
