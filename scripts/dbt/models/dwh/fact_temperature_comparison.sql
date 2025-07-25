-- ##############################################
-- dbt SQL Model for `dwh.fact_temperature_comparison`
-- Compare current vs. forecast temperature per city per day
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'forecast_date'],
    schema = 'dwh',
    on_schema_change = 'sync_all_columns'
) }}

{% if is_incremental() %}
with max_loaded as (
    select coalesce(max(forecast_date), '2000-01-01') as max_loaded_date
    from {{ this }}
),
{% else %}
with max_loaded as (
    select '2000-01-01'::date as max_loaded_date
),
{% endif %}

forecast as (
    select *
    from {{ ref('forecast') }}
    where date > (select max_loaded_date from max_loaded)
),

current as (
    select *
    from {{ ref('current') }}
    where date > (select max_loaded_date from max_loaded)
),

casted_forecast as (
    select
        {{ cast_safe('location_id', 'text') }} as location_id,
        {{ cast_safe('date', 'date') }} as forecast_date,
        {{ cast_safe('avgtemp_c', 'float') }} as forecast_temp_c
    from forecast
),

casted_current as (
    select
        {{ cast_safe('location_id', 'text') }} as location_id,
        {{ cast_safe('date', 'date') }} as current_date,
        {{ cast_safe('temp_c', 'float') }} as current_temp_c
    from current
),

comparison as (
    select
        cur.location_id,
        cur.current_date,
        fct.forecast_date,
        cur.current_temp_c,
        fct.forecast_temp_c,
        round((cur.current_temp_c - fct.forecast_temp_c)::numeric, 2) as difference_temp_c,
        case
            when cur.current_temp_c > fct.forecast_temp_c then 'Higher'
            when cur.current_temp_c < fct.forecast_temp_c then 'Lower'
            else 'Equal'
        end as comparison,
        {{ current_timestamp() }} as load_process_dt
    from casted_current cur
    join casted_forecast fct
      on cur.location_id = fct.location_id
)

select * from comparison
