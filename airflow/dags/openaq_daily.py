"""
Daily openaq pipeline   

Consists of following tasks:   
1. fetch_parameter - fetches parameter data from openaq api
2. fetch_country - fetches country data from openaq api
3. fetch_location - fetches location data from openaq api and sends the list of locations
    we are interested in to fetch_sensor
4. fetch_sensor - fetches sensor data from openaq api for the locations passed by 
    fetch_location and sends the list of sensors we are interested in to fetch_measurement
5. fetch_measurement - fetches measurement data from openaq api for the period we are
    interested in for sensor that are passed to the task by fetch_sensor
6. dbt_run_bronze - executes dbt bronze models, moving the data from parquet files 
    to duckDB database located at data/warehouse.db
7. dbt_run_silver - executes dbt silver models on duckDB database
8. dbt_run_gold - executes dbt gold models on duckDB database
9. archive_files - archives the parquet files from table_name/new to table_name
    dbt will look only at files in table_name/new for all future runs (except initial runs)
10. dbt_test - runs all the data quality tests    

We are filtering and fetching only data for Balkan countries    

Data for the sensor and measurement is filtered based on last_extraction_timestamp which is saved at data/raw/last_extraction_timestamp.txt. If the file is not found (for example on the initial extraction) the data from the last 7 days is fetched.    

API calls are allowed to fail for maximum of MAX_RETRY times (3), after that the specific call gets skipped which is logged in the task logs    

API we are using has a 60 requests/minute cap, so the extraction jobs are limited by this.    
"""

import asyncio
import time
from pathlib import Path
import logging

import pendulum
from openaq import AsyncOpenAQ
import pandas as pd

from airflow.sdk import dag, task

LOG = logging.getLogger()

API_LIMIT_PER_MINUTE = 60

API_KEY = "b79228d202aabb8f23234309f91aaac63b69591b974870dfb7d37128df3ebfbc"

COUNTRY_IDS = [
    132,
    110,
    103,
    80,
    75,
    65,
    131,
    62,
    74,
    97,
    104,
]  # openaq's ids of all the balkan countries
BATCH_SIZE = 5
LIMIT = 1000
MAX_RETRY = 3
SAVE_PATH = "data/raw/"


class RateLimiter:
    semaphore = asyncio.Semaphore(API_LIMIT_PER_MINUTE)
    list = [time.time()] * API_LIMIT_PER_MINUTE
    lock = asyncio.Lock()

    async def acquire(amount=1):
        async with RateLimiter.lock:
            for _ in range(amount):
                await RateLimiter.semaphore.acquire()
                last_request = RateLimiter.list.pop(0)

            time_to_wait = last_request + 60 - time.time()
            if time_to_wait > 0:
                await asyncio.sleep(time_to_wait)

    def release():
        RateLimiter.list.append(time.time())
        RateLimiter.semaphore.release()


def save_data(df, output_dir, prefix="", suffix=""):
    if df.empty:
        return None

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = pendulum.now("UTC").strftime("%Y%m%d_%H%M%S%f")

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


async def fetch_data_and_save(output_dir, job_name, async_func, *args, **kwargs):
    await RateLimiter.acquire()

    response = await execute_with_retry(async_func, *args, **kwargs)

    if response is None:
        return

    RateLimiter.release()

    data = response.dict()
    df = pd.json_normalize(data["results"])

    if "locations_id" in kwargs:
        df["location_id"] = kwargs["locations_id"]

    extraction_timestamp = pendulum.now("UTC")
    df["extraction_timestamp"] = extraction_timestamp

    save_data(df, output_dir, prefix=job_name)


async def fetch_paged_data_and_save(
    output_dir, job_name, async_func, *args, batch_size=1, **kwargs
):
    results = True
    page_num = 0

    while results:
        await RateLimiter.acquire(batch_size)

        tasks = []
        for _ in range(batch_size):
            page_num += 1
            kwargs["page"] = page_num

            tasks.append(execute_with_retry(async_func, *args, **kwargs))

        error_count = 0

        async for task in asyncio.as_completed(tasks):
            response = task.result()

            if response is None:
                error_count += 1
                continue

            RateLimiter.release()

            if len(response.results) == 0:
                results = False
            else:
                data = response.dict()
                df = pd.json_normalize(data["results"])

                if "sensors_id" in kwargs:
                    df["sensor_id"] = kwargs["sensors_id"]

                extraction_timestamp = pendulum.now("UTC")
                df["extraction_timestamp"] = extraction_timestamp

                save_data(df, output_dir, prefix=job_name)

        if error_count == batch_size:
            break


async def execute_with_retry(async_func, *args, **kwargs):
    retry_count = 0
    while True:
        try:
            if retry_count != 0:
                await RateLimiter.acquire()
            return await async_func(*args, **kwargs)
        except Exception as err:
            RateLimiter.release()
            if retry_count < MAX_RETRY:
                retry_count += 1
                await asyncio.sleep(120 * retry_count)
            else:
                LOG.exception(
                    "A task "
                    + async_func.__name__
                    + "(args = "
                    + str(args)
                    + ", kwargs = "
                    + str(kwargs)
                    + ")"
                    + " has failed "
                    + str(MAX_RETRY + 1)
                    + " times. Code: "
                    + err.status_code
                    + " Error message: "
                    + err.message
                    + "\n"
                )
                return None


def compare_dates(datetime_from):
    def compare_(x):
        if x.datetime_last is not None:
            if isinstance(x.datetime_last, dict):
                return pendulum.parse(x.datetime_last["utc"]) > datetime_from
            return pendulum.parse(x.datetime_last.utc) > datetime_from
        return False

    return compare_


async def fetch_and_save_ids(api_call, id_list, datetime_from, *args, **kwargs):
    response = await api_call(*args, **kwargs)
    id_list.extend(
        map(lambda x: x.id, filter(compare_dates(datetime_from), response.results))
    )
    return response


@dag(
    dag_id="openaq_daily",
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    description="Daily run of openaq pipeline, fetching data from the api and then ingesting it into bronze, silver and gold.",
    tags=["openaq", "extraction", "dbt", "dbt test"],
)
def openaq_daily():
    @task()
    def fetch_location():
        async def fetch_location_():
            client = AsyncOpenAQ(api_key=API_KEY)

            last_extraction_timestamp_file_path = Path(
                SAVE_PATH + "last_extraction_timestamp.txt"
            )
            if Path.exists(last_extraction_timestamp_file_path):
                with open(last_extraction_timestamp_file_path, "r") as file:
                    datetime_from = pendulum.parse(file.read())
            else:
                datetime_from = pendulum.now("UTC").subtract(days=7)

            location_ids = []
            await fetch_paged_data_and_save(
                SAVE_PATH + "location/new",
                "location",
                fetch_and_save_ids,
                batch_size=BATCH_SIZE,
                api_call=client.locations.list,
                id_list=location_ids,
                datetime_from=datetime_from,
                countries_id=COUNTRY_IDS,
            )
            await client.close()

            return location_ids

        return asyncio.run(fetch_location_())

    @task()
    def fetch_parameter():
        async def fetch_parameter_():
            client = AsyncOpenAQ(api_key=API_KEY)

            await fetch_paged_data_and_save(
                SAVE_PATH + "parameter/new",
                "parameter",
                client.parameters.list,
                batch_size=BATCH_SIZE,
            )

            await client.close()

        asyncio.run(fetch_parameter_())

    @task()
    def fetch_country():
        async def fetch_country_():
            client = AsyncOpenAQ(api_key=API_KEY)

            await fetch_paged_data_and_save(
                SAVE_PATH + "country/new",
                "country",
                client.countries.list,
                batch_size=BATCH_SIZE,
            )

            await client.close()

        asyncio.run(fetch_country_())

    @task()
    def fetch_sensor(location_ids):
        async def fetch_sensor_(location_ids):
            client = AsyncOpenAQ(api_key=API_KEY)

            last_extraction_timestamp_file_path = Path(
                SAVE_PATH + "last_extraction_timestamp.txt"
            )
            if Path.exists(last_extraction_timestamp_file_path):
                with open(last_extraction_timestamp_file_path, "r") as file:
                    datetime_from = pendulum.parse(file.read())
            else:
                datetime_from = pendulum.now("UTC").subtract(days=7)

            sensor_ids = []

            tasks = []
            for locations_id in location_ids:
                tasks.append(
                    fetch_data_and_save(
                        SAVE_PATH + "sensor/new",
                        "sensor",
                        fetch_and_save_ids,
                        api_call=client.locations.sensors,
                        id_list=sensor_ids,
                        datetime_from=datetime_from,
                        locations_id=locations_id,
                    )
                )

            await asyncio.gather(*tasks)

            await client.close()

            return sensor_ids

        return asyncio.run(fetch_sensor_(location_ids))

    @task()
    def fetch_measurement(sensor_ids):
        async def fetch_measurement_(sensor_ids):
            client = AsyncOpenAQ(api_key=API_KEY)

            datetime_to = pendulum.now("UTC")

            last_extraction_timestamp_file_path = Path(
                SAVE_PATH + "last_extraction_timestamp.txt"
            )
            if Path.exists(last_extraction_timestamp_file_path):
                with open(last_extraction_timestamp_file_path, "r") as file:
                    datetime_from = pendulum.parse(file.read())
            else:
                datetime_from = datetime_to.subtract(days=7)

            tasks = []
            for sensors_id in sensor_ids:
                tasks.append(
                    fetch_paged_data_and_save(
                        SAVE_PATH + "measurement/new",
                        "measurement",
                        client.measurements.list,
                        sensors_id=sensors_id,
                        datetime_from=datetime_from,
                        datetime_to=datetime_to,
                    )
                )

            await asyncio.gather(*tasks)

            with open(last_extraction_timestamp_file_path, "w") as file:
                file.write(datetime_to.to_atom_string())

            await client.close()

        asyncio.run(fetch_measurement_(sensor_ids))

    @task.bash()
    def dbt_run_bronze():
        return "cd $AIRFLOW_HOME/../dbt && dbt run --select bronze"
    
    @task.bash()
    def dbt_run_silver():
        return "cd $AIRFLOW_HOME/../dbt && dbt run --select silver"

    @task.bash()
    def dbt_run_gold():
        return "cd $AIRFLOW_HOME/../dbt && dbt run --select gold"


    @task.bash()
    def archive_files():
        return "cd $AIRFLOW_HOME/../data/raw && mv location/new/* location/ && mv parameter/new/* parameter/ && mv country/new/* country/ && mv sensor/new/* sensor/ && mv measurement/new/* measurement/"
    
    @task.bash()
    def dbt_test():
        return "cd $AIRFLOW_HOME/../dbt && dbt test"

    location_ids = fetch_location()
    fetch_parameter = fetch_parameter()
    fetch_country = fetch_country()
    sensor_ids = fetch_sensor(location_ids)
    fetch_measurement = fetch_measurement(sensor_ids)

    dbt_run_bronze = dbt_run_bronze()
    dbt_run_silver = dbt_run_silver()
    dbt_run_gold = dbt_run_gold()

    archive_files = archive_files()

    dbt_test = dbt_test()
    (
        fetch_parameter
        >> fetch_country
        >> location_ids
        >> sensor_ids
        >> fetch_measurement
        >> dbt_run_bronze
        >> dbt_run_silver
        >> dbt_run_gold
        >> archive_files
        >> dbt_test
    )


openaq_daily()

# TODO:
#       documentation
