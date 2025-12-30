{{ config(materialized='incremental', alias='country', unique_key='id', schema='bronze') }}

{% if not is_incremental() %}

select 
	id,
        code,
 	name,
	datetime_first,
	datetime_last,
       	parameters,
	current_timestamp as dbt_load_timestamp

from '../data/raw/country/**/*.parquet'
where (id, extraction_timestamp) in (select id, max(extraction_timestamp) from '../data/raw/country/**/*.parquet' group by id)
{% elif files_exist('../data/raw/country/new/*.parquet') %}

    select
        id,
        code,
        name,
        datetime_first,
        datetime_last,
        parameters,
        current_timestamp as dbt_load_timestamp

    from '../data/raw/country/new/*.parquet'
    where
        extraction_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% else %}

    select
        id,
        code,
        name,
        datetime_first,
        datetime_last,
        parameters,
        current_timestamp as dbt_load_timestamp
    from {{ this }}
    where 1 = 0

{% endif %}
