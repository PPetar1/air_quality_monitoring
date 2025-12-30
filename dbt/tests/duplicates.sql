{{ config(store_failures = true) }}

select
    id,
    'bronze_country' as table_name,
    count(*) as count
from {{ ref('bronze_country') }}
group by id
having count(*) > 1
union
select
    id,
    'bronze_parameter' as table_name,
    count(*) as count
from {{ ref('bronze_parameter') }}
group by id
having count(*) > 1
union
select
    id,
    'bronze_location' as table_name,
    count(*) as count
from {{ ref('bronze_location') }}
group by id
having count(*) > 1
union
select
    id,
    'bronze_sensor' as table_name,
    count(*) as count
from {{ ref('bronze_sensor') }}
group by id
having count(*) > 1
union
select
    concat(
        sensor_id,
        '|',
        parameter_id,
        '|',
        period_datetime_from_utc,
        '|',
        period_datetime_to_utc
    ),
    'bronze_measurement' as table_name,
    count(*) as count
from {{ ref('bronze_measurement') }}
group by
    sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc
having count(*) > 1
union
select
    id,
    'silver_country' as table_name,
    count(*) as count
from {{ ref('silver_country') }}
group by id
having count(*) > 1
union
select
    id,
    'silver_parameter' as table_name,
    count(*) as count
from {{ ref('silver_parameter') }}
group by id
having count(*) > 1
union
select
    id,
    'silver_location' as table_name,
    count(*) as count
from {{ ref('silver_location') }}
group by id
having count(*) > 1
union
select
    id,
    'silver_sensor' as table_name,
    count(*) as count
from {{ ref('silver_sensor') }}
group by id
having count(*) > 1
union
select
    concat(
        sensor_id,
        '|',
        parameter_id,
        '|',
        period_datetime_from_utc,
        '|',
        period_datetime_to_utc
    ),
    'silver_measurement' as table_name,
    count(*) as count
from {{ ref('silver_measurement') }}
group by
    sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc
having count(*) > 1
union
select
    id,
    'gold_country' as table_name,
    count(*) as count
from {{ ref('gold_country') }}
group by id
having count(*) > 1
union
select
    id,
    'gold_parameter' as table_name,
    count(*) as count
from {{ ref('gold_parameter') }}
group by id
having count(*) > 1
union
select
    id,
    'gold_location' as table_name,
    count(*) as count
from {{ ref('gold_location') }}
group by id
having count(*) > 1
union
select
    id,
    'gold_locality' as table_name,
    count(*) as count
from {{ ref('gold_locality') }}
group by id
having count(*) > 1
union
select
    id,
    'gold_sensor' as table_name,
    count(*) as count
from {{ ref('gold_sensor') }}
group by id
having count(*) > 1
union
select
    concat(
        sensor_id,
        '|',
        parameter_id,
        '|',
        period_datetime_from_utc,
        '|',
        period_datetime_to_utc
    ),
    'gold_measurement' as table_name,
    count(*) as count
from {{ ref('gold_measurement') }}
group by
    sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc
having count(*) > 1
