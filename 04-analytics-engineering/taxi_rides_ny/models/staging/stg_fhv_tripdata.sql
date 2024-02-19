{{ config(materialized='view') }}

with taxitrips as (
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("string")) }} as sr_flag,
    Affiliated_base_number,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
from {{ source('staging','fhv_tripdata') }}
)

select * from taxitrips
where (extract(year from pickup_datetime) = 2019)

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}

