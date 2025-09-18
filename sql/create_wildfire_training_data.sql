-- Enhanced wildfire training data with longitude and latitude coordinates
-- These coordinates are for data enrichment and geographical analysis only
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
    CONCAT('station_', CAST(FLOOR(RAND() * 16) + 1 AS STRING)) AS station_id,
    CAST(RAND() * 1500 + 50 AS FLOAT64) AS elevation_meters,

    -- Categorical features
    CASE
      WHEN RAND() < 0.2 THEN 'sunny'
      WHEN RAND() < 0.4 THEN 'cloudy'
      WHEN RAND() < 0.6 THEN 'windy'
      WHEN RAND() < 0.8 THEN 'rainy'
      ELSE 'stormy'
    END AS weather_condition,

    -- All 16 German states (Bundesländer) with random assignment
    CASE
      WHEN RAND() < 0.0625 THEN 'Baden-Württemberg'
      WHEN RAND() < 0.125 THEN 'Bayern'
      WHEN RAND() < 0.1875 THEN 'Berlin'
      WHEN RAND() < 0.25 THEN 'Brandenburg'
      WHEN RAND() < 0.3125 THEN 'Bremen'
      WHEN RAND() < 0.375 THEN 'Hamburg'
      WHEN RAND() < 0.4375 THEN 'Hessen'
      WHEN RAND() < 0.5 THEN 'Mecklenburg-Vorpommern'
      WHEN RAND() < 0.5625 THEN 'Niedersachsen'
      WHEN RAND() < 0.625 THEN 'Nordrhein-Westfalen'
      WHEN RAND() < 0.6875 THEN 'Rheinland-Pfalz'
      WHEN RAND() < 0.75 THEN 'Saarland'
      WHEN RAND() < 0.8125 THEN 'Sachsen'
      WHEN RAND() < 0.875 THEN 'Sachsen-Anhalt'
      WHEN RAND() < 0.9375 THEN 'Schleswig-Holstein'
      ELSE 'Thüringen'
    END AS state_province,

    CAST(FLOOR(RAND() * 12) + 1 AS INT64) AS month
  FROM
    UNNEST(GENERATE_ARRAY(1, ${training_data_size})) -- Generate configurable rows
),
-- Add coordinates based on state_province
data_with_coordinates AS (
  SELECT
    *,
    -- Add longitude and latitude based on German state centers with some random variation
    CASE state_province
      WHEN 'Baden-Württemberg' THEN 9.0 + (RAND() - 0.5) * 2.5  -- Center around Stuttgart
      WHEN 'Bayern' THEN 11.5 + (RAND() - 0.5) * 4.0            -- Center around Munich
      WHEN 'Berlin' THEN 13.4 + (RAND() - 0.5) * 0.5            -- Berlin area
      WHEN 'Brandenburg' THEN 13.0 + (RAND() - 0.5) * 2.0       -- Around Potsdam
      WHEN 'Bremen' THEN 8.8 + (RAND() - 0.5) * 0.3             -- Bremen city
      WHEN 'Hamburg' THEN 10.0 + (RAND() - 0.5) * 0.3           -- Hamburg city
      WHEN 'Hessen' THEN 9.0 + (RAND() - 0.5) * 2.0             -- Center around Frankfurt
      WHEN 'Mecklenburg-Vorpommern' THEN 12.5 + (RAND() - 0.5) * 2.5  -- Schwerin area
      WHEN 'Niedersachsen' THEN 9.5 + (RAND() - 0.5) * 3.0      -- Hannover area
      WHEN 'Nordrhein-Westfalen' THEN 7.5 + (RAND() - 0.5) * 2.5  -- Düsseldorf area
      WHEN 'Rheinland-Pfalz' THEN 7.5 + (RAND() - 0.5) * 2.0    -- Mainz area
      WHEN 'Saarland' THEN 7.0 + (RAND() - 0.5) * 0.5           -- Saarbrücken area
      WHEN 'Sachsen' THEN 13.5 + (RAND() - 0.5) * 2.0           -- Dresden area
      WHEN 'Sachsen-Anhalt' THEN 11.5 + (RAND() - 0.5) * 2.0    -- Magdeburg area
      WHEN 'Schleswig-Holstein' THEN 9.5 + (RAND() - 0.5) * 2.0  -- Kiel area
      WHEN 'Thüringen' THEN 11.0 + (RAND() - 0.5) * 1.5         -- Erfurt area
      ELSE 10.0 + (RAND() - 0.5) * 2.0
    END AS longitude,

    CASE state_province
      WHEN 'Baden-Württemberg' THEN 48.5 + (RAND() - 0.5) * 2.0  -- Stuttgart latitude
      WHEN 'Bayern' THEN 48.5 + (RAND() - 0.5) * 3.0            -- Bavaria range
      WHEN 'Berlin' THEN 52.5 + (RAND() - 0.5) * 0.3            -- Berlin latitude
      WHEN 'Brandenburg' THEN 52.3 + (RAND() - 0.5) * 2.0       -- Brandenburg range
      WHEN 'Bremen' THEN 53.1 + (RAND() - 0.5) * 0.2            -- Bremen latitude
      WHEN 'Hamburg' THEN 53.6 + (RAND() - 0.5) * 0.2           -- Hamburg latitude
      WHEN 'Hessen' THEN 50.5 + (RAND() - 0.5) * 2.0            -- Hesse range
      WHEN 'Mecklenburg-Vorpommern' THEN 53.5 + (RAND() - 0.5) * 1.5  -- MV range
      WHEN 'Niedersachsen' THEN 52.5 + (RAND() - 0.5) * 2.5     -- Lower Saxony
      WHEN 'Nordrhein-Westfalen' THEN 51.5 + (RAND() - 0.5) * 2.0  -- NRW range
      WHEN 'Rheinland-Pfalz' THEN 50.0 + (RAND() - 0.5) * 2.0   -- Rhineland-Palatinate
      WHEN 'Saarland' THEN 49.4 + (RAND() - 0.5) * 0.5          -- Saarland
      WHEN 'Sachsen' THEN 51.0 + (RAND() - 0.5) * 1.5           -- Saxony
      WHEN 'Sachsen-Anhalt' THEN 51.8 + (RAND() - 0.5) * 1.5    -- Saxony-Anhalt
      WHEN 'Schleswig-Holstein' THEN 54.2 + (RAND() - 0.5) * 1.5  -- Schleswig-Holstein
      WHEN 'Thüringen' THEN 50.8 + (RAND() - 0.5) * 1.2         -- Thuringia
      ELSE 51.0 + (RAND() - 0.5) * 2.0
    END AS latitude
  FROM synthetic_data
)
-- Apply rules to label the data and include coordinates
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
FROM data_with_coordinates;