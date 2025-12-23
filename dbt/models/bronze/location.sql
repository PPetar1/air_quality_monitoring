{{ config(materialized='table', schema='bronze') }}

select *, current_timestamp as dbt_load_timestamp
from read_parquet('../data/raw/location/new/*.parquet')
