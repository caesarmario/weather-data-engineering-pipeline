-- ##############################################
-- dbt SQL Model for `dwh.fact_weather_alerts`
-- Identify weather alerts based on specific thresholds
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
    select * from {{ ref('forecast') }}

    {% if is_incremental() %}
      where date > (select coalesce(max(date), '2000-01-01') from {{ this }})
    {% endif %}
),

casted as (
    select
        location_id,
        date,
        {{ cast_safe('maxtemp_c', 'float') }} as maxtemp_c,
        {{ cast_safe('totalprecip_mm', 'float') }} as totalprecip_mm,
        {{ cast_safe('maxwind_kph', 'float') }} as maxwind_kph
    from forecast
),

flagged as (
    select
        *,
        (maxtemp_c > 35) as alert_extreme_heat,
        ((totalprecip_mm > 20) or (maxwind_kph > 15)) as alert_storm,
        current_timestamp::date as load_process_dt
    from casted
),

final as (
    select
        location_id,
        date,
        maxtemp_c,
        totalprecip_mm,
        maxwind_kph,
        alert_extreme_heat,
        alert_storm,
        load_process_dt
    from flagged
    where alert_extreme_heat = true or alert_storm = true
)

select * from final
