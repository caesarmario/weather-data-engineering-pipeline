-- ##############################################
-- dbt SQL Model for `dwh.fact_temperature_statistics`
-- Identify weather temperature statistics in each location
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(
    materialized = 'incremental',
    unique_key = 'location_id',
    schema = 'dwh',
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    partition_by = {
      "field": "load_process_dt",
      "data_type": "date"
    }
) }}

WITH base_data AS (

    SELECT *
    FROM {{ ref('forecast') }}

),

numeric_cast AS (

    SELECT
        location_id,
        CAST(date AS DATE) AS date,
        CAST(maxtemp_c AS FLOAT) AS maxtemp_c,
        CAST(mintemp_c AS FLOAT) AS mintemp_c,
        CAST(avgtemp_c AS FLOAT) AS avgtemp_c,
        CAST(maxtemp_f AS FLOAT) AS maxtemp_f,
        CAST(mintemp_f AS FLOAT) AS mintemp_f,
        CAST(avgtemp_f AS FLOAT) AS avgtemp_f
    FROM base_data

),

aggregated AS (

    SELECT
        location_id,
        MIN(mintemp_c) AS min_temp_c,
        MAX(maxtemp_c) AS max_temp_c,
        AVG(avgtemp_c) AS avg_temp_c,
        MIN(mintemp_f) AS min_temp_f,
        MAX(maxtemp_f) AS max_temp_f,
        AVG(avgtemp_f) AS avg_temp_f,
        MIN(date) AS date_range_start,
        MAX(date) AS date_range_end,
        MAX(date) - MIN(date) + 1 AS total_days,
        {{ current_timestamp() }}::DATE AS load_process_dt
    FROM numeric_cast
    GROUP BY location_id

),

final AS (

    SELECT
        *,
        max_temp_c - min_temp_c AS temperature_variation_c,
        max_temp_f - min_temp_f AS temperature_variation_f
    FROM aggregated

)

SELECT * FROM final

{% if is_incremental() %}
WHERE load_process_dt > (
    SELECT COALESCE(MAX(load_process_dt), '2000-01-01') FROM {{ this }}
)
{% endif %}
