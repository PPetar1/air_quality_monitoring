{{ config(materialized='incremental', alias='country', unique_key='id', schema='bronze') }}

select 
	id,
        code,
 	name,
	datetime_first,
	datetime_last,
       	parameters,
	current_timestamp as dbt_load_timestamp

from read_parquet('../data/raw/country/new/*.parquet')

{% if is_incremental() %}

where extraction_timestamp >= (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
