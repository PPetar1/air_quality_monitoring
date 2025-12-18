#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
from pathlib import Path
from openaq import OpenAQ

API_KEY = "28167e861b5b17b41ef6f9e4ab183790ae37b8df75697bbb95212edde98e215d"

def save_data(df, output_dir="data", prefix="", suffix=""):
    if df.empty:
        return None
    
    output_path = Path(output_dir) / "raw"
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    filename = f"{prefix}{timestamp}{suffix}.parquet"
    parquet_file = output_path / filename
    df.to_parquet(parquet_file, index=False)
    
    return parquet_file

def main():
    client = OpenAQ(api_key=API_KEY)

    response = client.locations.list(parameters_id=2, limit=1000)
    data = response.dict()
    df = pd.json_normalize(data['results'])
    save_data(df)
    client.close()

if __name__ == "__main__":
    main()
