{{ config(
    materialized='table'
) }}

with color_trips as
(
    select 
    tripid,
    service_type,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationid,
    pickup_zone,
    pickup_borough,
    dropoff_locationid,
    dropoff_zone,
    dropoff_borough
    from {{ ref("fact_trips")}}
),

fhv_trips as
(
    select 
    tripid,
    service_type,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationid,
    pickup_zone,
    pickup_borough,
    dropoff_locationid,
    dropoff_zone,
    dropoff_borough
    from {{ ref("fact_fhv_trips")}}
)

-- create teable
select * 
from fhv_trips
union all 
select * 
from color_trips

