-- ##############################################
-- dbt SQL Model for `l1_weather.l1_current`
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = "view",
    schema = "l1_weather"
) }}

with source as (

    select * from {{ source("l0_weather", "current") }}

),

typed as (
    select
        {{ cast_safe("location_id", "text") }} as location_id,
        {{ cast_safe("date", "date") }} as date,
        {{ cast_safe("last_updated", "timestamp") }} as last_updated,
        {{ cast_safe("temp_c", "float") }} as temp_c,
        {{ cast_safe("temp_f", "float") }} as temp_f,
        {{ cast_safe("is_day", "boolean") }} as is_day,
        {{ cast_safe("condition_text", "text") }} as condition_text,
        {{ cast_safe("condition_code", "int") }} as condition_code,
        {{ cast_safe("batch_id", "text") }} as batch_id,
        {{ dbt_utils.current_timestamp() }} as load_dt
    from source
)

select * from typed
