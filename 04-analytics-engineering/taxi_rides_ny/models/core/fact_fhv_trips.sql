{{ config(
    materialized='table'
) }}

-- Define FHV trip data as a CTE
with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 

-- Define dimension zones as a CTE
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
-- create table
select
    fhv.tripid,
    "fhv" as service_type,
    fhv.pickup_datetime,
    fhv.dropoff_datetime,
    fhv.dispatching_base_num,
    fhv.pickup_locationid,
    pickup_zone.zone as pickup_zone,
    pickup_zone.borough as pickup_borough,
    fhv.dropoff_locationid,
    dropoff_zone.zone as dropoff_zone,
    dropoff_zone.borough as dropoff_borough,
    fhv.sr_flag,
    fhv.Affiliated_base_number
from fhv_tripdata as fhv
inner join dim_zones as pickup_zone 
on fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dropoff_locationid = dropoff_zone.locationid