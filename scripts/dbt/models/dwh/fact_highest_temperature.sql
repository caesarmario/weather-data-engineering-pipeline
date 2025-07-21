-- ##############################################
-- dbt SQL Model for `dwh.fact_highest_temperature`
-- Identify max temp per city from forecast table
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'date'],
    schema = 'dwh',
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    partition_by = {
      "field": "load_process_dt",
      "data_type": "date"
    }
) }}

with forecast as (
    select *
    from {{ source('l0_weather', 'forecast') }}

    {% if is_incremental() %}
      where load_dt > (select coalesce(max(load_process_dt), '2000-01-01') from {{ this }})
    {% endif %}
),

casted as (
    select
        {{ cast_safe('location_id', 'text') }} as location_id,
        {{ cast_safe('date', 'date') }} as date,
        {{ cast_safe('maxtemp_c', 'float') }} as maxtemp_c,
        {{ current_timestamp() }}::date as load_process_dt
    from forecast
),

ranked as (
    select *,
           row_number() over (partition by location_id order by maxtemp_c desc, date asc) as rn
    from casted
)

select location_id, date, maxtemp_c, load_process_dt
from ranked
where rn = 1
