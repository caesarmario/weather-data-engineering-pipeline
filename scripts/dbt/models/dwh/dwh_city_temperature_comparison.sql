-- ##############################################
-- dbt SQL Model for dwh.dwh_city_temperature_comparison daily
-- Comparing daily stats of temperatures in each cities
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = 'table',
    schema = 'dwh',
    unique_key = 'name'
) }}

with forecast as (
    select
        l.name,
        avg(f.maxtemp_c) as avg_max_temp_c,
        avg(f.mintemp_c) as avg_min_temp_c,
        avg(f.avgtemp_c) as avg_forecast_temp_c
    from {{ ref('forecast') }} f
    join {{ ref('location') }} l on f.location_id = l.location_id
    group by l.name
),

current as (
    select
        l.name,
        avg(c.temp_c) as avg_current_temp_c
    from {{ ref('current') }} c
    join {{ ref('location') }} l on c.location_id = l.location_id
    group by l.name
),

combined as (
    select
        f.name,
        c.avg_current_temp_c,
        f.avg_max_temp_c,
        f.avg_min_temp_c,
        f.avg_forecast_temp_c,
        {{ current_timestamp() }} as load_dt
    from forecast f
    left join current c on f.name = c.name
)

select * from combined