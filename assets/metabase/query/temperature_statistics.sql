-- ##############################################
-- Metabase SQL query for Temperature Statistics Dashboard
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

-- Average Temperature Trends per City
SELECT
  location_id,
  avg_temp_c,
  date_range_start
FROM dwh.fact_temperature_statistics
ORDER BY location_id, date_range_start;

-- Top 5 Cities by Highest Average Temperature (Last 7 Days)
SELECT
  location_id,
  ROUND(AVG(avg_temp_c)::numeric, 2) AS avg_temperature
FROM dwh.fact_temperature_statistics
WHERE date_range_start >= CURRENT_DATE - INTERVAL '7 day'
GROUP BY location_id
ORDER BY avg_temperature DESC
LIMIT 5;

-- Temperature Fluctuation (Max - Min °C) per City
SELECT
  location_id,
  date_range_start,
  ROUND((max_temp_c - min_temp_c)::numeric, 2) AS fluctuation_c
FROM dwh.fact_temperature_statistics
ORDER BY location_id, date_range_start;

-- Heat Alert Count
SELECT
  location_id,
  COUNT(*) AS heat_alert_days
FROM dwh.fact_temperature_statistics
WHERE max_temp_c > 35
GROUP BY location_id
ORDER BY heat_alert_days DESC;

-- WoW Change in Avg Temperature per City
WITH base AS (
  SELECT
    location_id,
    DATE_TRUNC('week', date_range_start) AS week,
    ROUND(AVG(avg_temp_c)::numeric, 2) AS weekly_avg_temp
  FROM dwh.fact_temperature_statistics
  GROUP BY location_id, week
),
lagged AS (
  SELECT *,
         LAG(weekly_avg_temp) OVER (PARTITION BY location_id ORDER BY week) AS prev_week_temp
  FROM base
)
SELECT
  location_id,
  week,
  weekly_avg_temp,
  prev_week_temp,
  ROUND((weekly_avg_temp - prev_week_temp)::numeric, 2) AS wow_change
FROM lagged
WHERE prev_week_temp IS NOT NULL;
