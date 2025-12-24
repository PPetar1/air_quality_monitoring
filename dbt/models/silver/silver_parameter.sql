{{ config(materialized='incremental', alias='parameter', unique_key='id', schema='silver') }}

select 
	id,
	name,
	units,
	display_name,
	description,
	current_timestamp as dbt_load_timestamp

from {{ ref('bronze_parameter') }}

{% if is_incremental() %}

where dbt_load_timestamp = (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
