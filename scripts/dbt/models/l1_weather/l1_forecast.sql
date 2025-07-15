-- ##############################################
-- dbt SQL Query for `forecast` table
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = "view",
    schema = "l1_weather"
) }}

with source as (

    select * from {{ source("l0_weather", "forecast") }}

),

typed as (

    select
        {{ cast_safe("location_id", "text") }} as location_id,
        {{ cast_safe("date", "date") }} as date,
        {{ cast_safe("date_epoch", "int") }} as date_epoch,
        {{ cast_safe("maxtemp_c", "float") }} as maxtemp_c,
        {{ cast_safe("maxtemp_f", "float") }} as maxtemp_f,
        {{ cast_safe("mintemp_c", "float") }} as mintemp_c,
        {{ cast_safe("mintemp_f", "float") }} as mintemp_f,
        {{ cast_safe("avgtemp_c", "float") }} as avgtemp_c,
        {{ cast_safe("avgtemp_f", "float") }} as avgtemp_f,
        {{ cast_safe("maxwind_mph", "float") }} as maxwind_mph,
        {{ cast_safe("maxwind_kph", "float") }} as maxwind_kph,
        {{ cast_safe("totalprecip_mm", "float") }} as totalprecip_mm,
        {{ cast_safe("totalprecip_in", "float") }} as totalprecip_in,
        {{ cast_safe("totalsnow_cm", "float") }} as totalsnow_cm,
        {{ cast_safe("avgvis_km", "float") }} as avgvis_km,
        {{ cast_safe("avgvis_miles", "float") }} as avgvis_miles,
        {{ cast_safe("avghumidity", "int") }} as avghumidity,
        {{ cast_safe("daily_will_it_rain", "boolean") }} as daily_will_it_rain,
        {{ cast_safe("daily_chance_of_rain", "int") }} as daily_chance_of_rain,
        {{ cast_safe("daily_will_it_snow", "boolean") }} as daily_will_it_snow,
        {{ cast_safe("daily_chance_of_snow", "int") }} as daily_chance_of_snow,
        {{ cast_safe("condition_text", "text") }} as condition_text,
        {{ cast_safe("condition_code", "int") }} as condition_code,
        {{ cast_safe("uv", "float") }} as uv,
        {{ cast_safe("batch_id", "text") }} as batch_id,
        {{ dbt_utils.current_timestamp() }} as load_dt

    from source

)

select * from typed
