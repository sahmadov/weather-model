CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.wildfire_training_data` AS
WITH synthetic_data AS (
  SELECT
    ROW_NUMBER() OVER() AS row_id,
    -- Generate synthetic values for each feature
    CAST(RAND() * 25 + 10 AS FLOAT64) AS temperature_celsius,
    CAST(RAND() * 70 + 20 AS FLOAT64) AS humidity_percent,
    CAST(RAND() * 20 + 990 AS FLOAT64) AS pressure_hpa,
    CAST(RAND() * 15 AS FLOAT64) AS wind_speed_ms,
    CAST(RAND() * 360 AS FLOAT64) AS wind_direction_degrees,
    CAST(RAND() * 5 AS FLOAT64) AS precipitation_mm,
    CAST(RAND() * 10 AS FLOAT64) AS visibility_km,

    -- Assign a synthetic station ID and elevation
    CONCAT('station_', CAST(FLOOR(RAND() * 10) + 1 AS STRING)) AS station_id,
    CAST(RAND() * 1500 + 50 AS FLOAT64) AS elevation_meters,

    -- Categorical features
    CASE
      WHEN RAND() < 0.2 THEN 'sunny'
      WHEN RAND() < 0.4 THEN 'cloudy'
      WHEN RAND() < 0.6 THEN 'windy'
      WHEN RAND() < 0.8 THEN 'rainy'
      ELSE 'stormy'
    END AS weather_condition,

    CASE
      WHEN RAND() < 0.3 THEN 'California'
      WHEN RAND() < 0.6 THEN 'Colorado'
      ELSE 'Texas'
    END AS state_province,

    CAST(FLOOR(RAND() * 12) + 1 AS INT64) AS month
  FROM
    UNNEST(GENERATE_ARRAY(1, ${training_data_size})) -- Generate configurable rows
)
-- Apply rules to label the data
SELECT
  *,
  -- Derived risk features
  CASE
    WHEN temperature_celsius > 25 THEN 4
    WHEN temperature_celsius > 20 THEN 3
    ELSE 2
  END AS temp_risk_score,

  CASE
    WHEN humidity_percent < 40 THEN 4
    WHEN humidity_percent < 60 THEN 3
    ELSE 2
  END AS humidity_risk_score,

  CASE
    WHEN temperature_celsius > 30 AND humidity_percent < 30 AND wind_speed_ms > 10 THEN 'high'
    WHEN temperature_celsius > 25 AND humidity_percent < 50 THEN 'moderate'
    ELSE 'low'
  END AS wildfire_risk_label
FROM synthetic_data;