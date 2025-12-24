import pendulum
import asyncio
from openaq import AsyncOpenAQ
import pandas as pd
import time
from pathlib import Path
import os
import sys
import aiologic

from airflow.sdk import dag, task


class RateLimiter:
    def __init__(self, max_per_minute):
        self.semaphore = aiologic.Semaphore(max_per_minute)
        self.list = [0]*max_per_minute
        self.lock = aiologic.Lock()

    async def acquire(self, amount=1):
        async with self.lock:
            for _ in range(amount):
                await self.semaphore.async_acquire()
                last_request = self.list.pop(0)

            time_to_wait = last_request + 60 - time.time()
            if time_to_wait > 0:
                await asyncio.sleep(time_to_wait)

    def release(self):
        self.list.append(time.time())
        self.semaphore.release()

def save_data(df, output_dir, prefix="", suffix=""):
    if df.empty:
        return None
    
    output_path = Path(output_dir) 
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = pendulum.now('UTC').strftime("%Y%m%d_%H%M%S%f")

    prefix_ = ""
    suffix_ = ""
    if prefix != "":
        prefix_ = "_"
    if suffix != "":
        suffix_ = "_"

    filename = f"{prefix}{prefix_}{timestamp}{suffix_}{suffix}.parquet"
    parquet_file = output_path / filename
    df.to_parquet(parquet_file, index=False)
    
    return parquet_file

async def fetch_data_and_save(output_dir, job_name, rate_limiter, max_retry, async_func, *args, **kwargs):
    await rate_limiter.acquire()
    
    try:
        response = await execute_with_retry(async_func, rate_limiter, max_retry, *args, **kwargs) 
    except Exception as err:
        sys.stderr.write("A task has failed " + str(max_retry + 1) + " times. Error message: " + str(err) + "\n") #TODO: Logging
        return

    rate_limiter.release()

    data = response.dict()
    df = pd.json_normalize(data['results'])

    if 'locations_id' in kwargs:
        df['location_id'] = kwargs['locations_id']

    extraction_timestamp = pendulum.now('UTC')
    df['extraction_timestamp'] = extraction_timestamp

    save_data(df, output_dir, prefix=job_name)


async def fetch_paged_data_and_save(output_dir, job_name, rate_limiter, max_retry, async_func, batch_size=1, *args, **kwargs):
    results = True
    page_num = 0

    while results:
        await rate_limiter.acquire(batch_size)
        
        tasks = []
        for _ in range(batch_size):
            page_num += 1
            kwargs['page'] = page_num

            tasks.append(execute_with_retry(async_func, rate_limiter, max_retry, *args, **kwargs))
        
        error_count = 0

        async for task in asyncio.as_completed(tasks):
            try:
                response = task.result() 
            except Exception as err:
                sys.stderr.write("A task has failed " + str(max_retry + 1) + " times. Error message: " + str(err) + "\n") #TODO: Logging
                error_count += 1
                continue
                
            rate_limiter.release()

            if len(response.results) == 0:
                results = False
            else:
                data = response.dict()
                df = pd.json_normalize(data['results'])
            
                if 'sensors_id' in kwargs:
                    df['sensor_id'] = kwargs['sensors_id']
            
                extraction_timestamp = pendulum.now('UTC')
                df['extraction_timestamp'] = extraction_timestamp

                save_data(df, output_dir, prefix=job_name)

        if error_count == batch_size:
            break

async def execute_with_retry(async_func, rate_limiter, max_retry, *args, **kwargs):
    retry_count = 0
    while True:
        try:
            if retry_count != 0:
                await rate_limiter.acquire()
            return await async_func(*args, **kwargs)
        except Exception as err:
            rate_limiter.release()
            if retry_count < max_retry:
                retry_count += 1
            else:
                raise err
   
async def fetch_and_save_ids(api_call, id_list, *args, **kwargs):
    response = await api_call(*args, **kwargs)
    id_list.extend(map(lambda x: x.id, response.results))
    return response

@dag(
    dag_id="openaq_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 12, 10, tz="UTC"),
    catchup=False,
    description="Daily run of openaq pipeline, fetching data from the api and then ingesting it into bronze, silver and gold.",
    tags=["openaq"],
)
def openaq_daily():
    @task(multiple_outputs=True)    
    def initial_setup():
        API_KEY = "28167e861b5b17b41ef6f9e4ab183790ae37b8df75697bbb95212edde98e215d"
        COUNTRY_IDS = [132, 110, 103, 80, 75, 65, 131, 62, 74, 97, 104] #ids of all the balkan countries
        BATCH_SIZE = 5
        LIMIT = 1000
        MAX_RETRY = 3
        API_LIMIT_PER_MINUTE = 60
        SAVE_PATH = "data/raw/"
        RATE_LIMITER = RateLimiter(API_LIMIT_PER_MINUTE)

        CLIENT = AsyncOpenAQ(api_key=API_KEY)

        return {"COUNTRY_IDS": COUNTRY_IDS, "BATCH_SIZE": BATCH_SIZE, "LIMIT": LIMIT, "MAX_RETRY": MAX_RETRY, "SAVE_PATH": SAVE_PATH, "RATE_LIMITER": RATE_LIMITER, "CLIENT": CLIENT}

    @task()
    def fetch_location(SAVE_PATH, CLIENT, RATE_LIMITER, MAX_RETRY, BATCH_SIZE, COUNTRY_IDS):
        location_ids = []
        asyncio.run(fetch_paged_data_and_save(SAVE_PATH + "location/new", "location", RATE_LIMITER, MAX_RETRY, fetch_and_save_ids, api_call=CLIENT.locations.list, id_list=location_ids, batch_size=BATCH_SIZE, countries_id=COUNTRY_IDS))
        return location_ids

    @task()
    def fetch_parameter(SAVE_PATH, CLIENT, RATE_LIMITER, MAX_RETRY, BATCH_SIZE):
        asyncio.run(fetch_paged_data_and_save(SAVE_PATH + "country/new", "country", RATE_LIMITER, MAX_RETRY, CLIENT.countries.list, batch_size=BATCH_SIZE))

    @task()
    def fetch_country(SAVE_PATH, CLIENT, RATE_LIMITER, MAX_RETRY, BATCH_SIZE):
        asyncio.run(fetch_paged_data_and_save(SAVE_PATH + "parameter/new", "parameter", RATE_LIMITER, MAX_RETRY, CLIENT.parameters.list, batch_size=BATCH_SIZE))

    @task()
    def fetch_sensor(SAVE_PATH, CLIENT, RATE_LIMITER, MAX_RETRY, BATCH_SIZE, location_ids):
        sensor_ids = []

        tasks = []
        for locations_id in location_ids:
            tasks.append(fetch_data_and_save(SAVE_PATH + "sensor/new", "sensor", RATE_LIMITER, MAX_RETRY, fetch_and_save_ids, api_call=CLIENT.locations.sensors, id_list=sensor_ids, locations_id=locations_id))

        asyncio.run(asyncio.gather(*tasks))

        return sensor_ids

    @task()
    def fetch_measurement(SAVE_PATH, CLIENT, RATE_LIMITER, MAX_RETRY, BATCH_SIZE, sensor_ids):
        last_extraction_timestamp_file_path = Path(SAVE_PATH + "last_extraction_timestamp.txt")
        if Path.exists(last_extraction_timestamp_file_path):
            with open(last_extraction_timestamp_file_path, "r") as file:
                datetime_from = pendulum.parse(file.read())
        else:
            datetime_from = {{ start_date }}

        datetime_to = pendulum.now('UTC')

        tasks = []
        for sensors_id in sensor_ids:
            tasks.append(fetch_paged_data_and_save(SAVE_PATH + "measurement/new", "measurement", RATE_LIMITER, MAX_RETRY, CLIENT.measurements.list, sensors_id=sensors_id, datetime_from=datetime_from, datetime_to=datetime_to))

        asyncio.run(asyncio.gather(*tasks))

        with open(last_extraction_timestamp_file_path, "w") as file:
            file.write(datetime_to)

    @task()
    def close_connection():
        asyncio.run(client.close())

    @task.bash()
    def dbt_run():
        return "cd ../dbt && dbt run"

    @task.bash()
    def archive_files():
        return "cd ../data/raw && mv location/new/* location/ && mv parameter/new/* parameter/ && mv country/new/* country/ && mv sensor/new/* sensor/ && mv measurement/new/* measurement/"

    global_vars = initial_setup()
    location_ids = fetch_location(global_vars["SAVE_PATH"], global_vars["CLIENT"], global_vars["RATE_LIMITER"], global_vars["MAX_RETRY"], global_vars["BATCH_SIZE"], global_vars["COUNTRY_IDS"])
    fetch_parameter = fetch_parameter(global_vars["SAVE_PATH"], global_vars["CLIENT"], global_vars["RATE_LIMITER"], global_vars["MAX_RETRY"], global_vars["BATCH_SIZE"])
    fetch_country = fetch_country(global_vars["SAVE_PATH"], global_vars["CLIENT"], global_vars["RATE_LIMITER"], global_vars["MAX_RETRY"], global_vars["BATCH_SIZE"])
    sensor_ids = fetch_sensor(global_vars["SAVE_PATH"], global_vars["CLIENT"], global_vars["RATE_LIMITER"], global_vars["MAX_RETRY"], global_vars["BATCH_SIZE"], location_ids)
    fetch_measurement = fetch_measurement(global_vars["SAVE_PATH"], global_vars["CLIENT"], global_vars["RATE_LIMITER"], global_vars["MAX_RETRY"], global_vars["BATCH_SIZE"], sensor_ids)
    close_connection = close_connection()
    dbt_run = dbt_run()
    archive_files = archive_files()

    [fetch_parameter, fetch_country, fetch_measurement] >> close_connection >> dbt_run >> archive_files

openaq_daily()

# TODO: 
#       logging,
#       documentation
