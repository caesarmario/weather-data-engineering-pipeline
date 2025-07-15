-- ##############################################
-- dbt SQL Query for `location` table
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = "view",
    schema = "l1_weather"
) }}

with source as (

    select * from {{ source("l0_weather", "location") }}

),

typed as (

    select
        {{ cast_safe("location_id", "text") }} as location_id,
        {{ cast_safe("name", "text") }} as name,
        {{ cast_safe("region", "text") }} as region,
        {{ cast_safe("country", "text") }} as country,
        {{ cast_safe("latitude", "float") }} as latitude,
        {{ cast_safe("longitude", "float") }} as longitude,
        {{ cast_safe("timezone_id", "text") }} as timezone_id,
        {{ cast_safe("localtime_epoch", "int") }} as localtime_epoch,
        {{ cast_safe("localtime", "timestamp") }} as localtime,
        {{ cast_safe("batch_id", "text") }} as batch_id,
        {{ dbt_utils.current_timestamp() }} as load_dt

    from source

)

select * from typed
