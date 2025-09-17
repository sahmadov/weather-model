CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.next_day_wildfire_predictions` AS
WITH next_day_data AS (
  SELECT * FROM UNNEST([
    STRUCT(
      'station_CA' AS station_id,
      'California' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'America/Los_Angeles') AS timestamp,
      32.5 AS temperature_celsius,
      28.0 AS humidity_percent,
      1010.5 AS pressure_hpa,
      18.0 AS wind_speed_ms,
      270.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      15.0 AS visibility_km,
      'sunny' AS weather_condition,
      550.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'America/Los_Angeles')) AS month
    ),
    STRUCT(
      'station_CO' AS station_id,
      'Colorado' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'America/Denver') AS timestamp,
      25.0 AS temperature_celsius,
      45.0 AS humidity_percent,
      1008.0 AS pressure_hpa,
      10.0 AS wind_speed_ms,
      180.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      20.0 AS visibility_km,
      'cloudy' AS weather_condition,
      1655.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'America/Denver')) AS month
    ),
    STRUCT(
      'station_TX' AS station_id,
      'Texas' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'America/Chicago') AS timestamp,
      30.0 AS temperature_celsius,
      55.0 AS humidity_percent,
      1012.0 AS pressure_hpa,
      5.0 AS wind_speed_ms,
      90.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      10.0 AS visibility_km,
      'sunny' AS weather_condition,
      250.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'America/Chicago')) AS month
    )
  ])
)
-- Perform feature engineering on the simulated data
, engineered_data AS (
  SELECT
    *,
    CASE
      WHEN temperature_celsius > 25 THEN 4
      WHEN temperature_celsius > 20 THEN 3
      ELSE 2
    END AS temp_risk_score,
    CASE
      WHEN humidity_percent < 40 THEN 4
      WHEN humidity_percent < 60 THEN 3
      ELSE 2
    END AS humidity_risk_score
  FROM next_day_data
)
SELECT
  *
FROM
  ML.PREDICT(MODEL `${project_id}.${dataset_id}.${model_name}`,
    TABLE engineered_data);