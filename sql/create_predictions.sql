CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.next_day_wildfire_predictions` AS
WITH next_day_data AS (
  SELECT * FROM UNNEST([
    -- Bavaria (Bayern) - Southern Germany, prone to dry conditions
    STRUCT(
      'station_BY' AS station_id,
      'Bayern' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      31.0 AS temperature_celsius,
      32.0 AS humidity_percent,
      1012.5 AS pressure_hpa,
      15.0 AS wind_speed_ms,
      225.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      18.0 AS visibility_km,
      'sunny' AS weather_condition,
      580.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Baden-Württemberg - Southwestern Germany
    STRUCT(
      'station_BW' AS station_id,
      'Baden-Württemberg' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      29.0 AS temperature_celsius,
      38.0 AS humidity_percent,
      1010.0 AS pressure_hpa,
      12.0 AS wind_speed_ms,
      180.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      15.0 AS visibility_km,
      'sunny' AS weather_condition,
      420.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Brandenburg - Around Berlin, continental climate
    STRUCT(
      'station_BB' AS station_id,
      'Brandenburg' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      27.0 AS temperature_celsius,
      42.0 AS humidity_percent,
      1008.0 AS pressure_hpa,
      8.0 AS wind_speed_ms,
      90.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      12.0 AS visibility_km,
      'cloudy' AS weather_condition,
      85.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Sachsen (Saxony) - Eastern Germany
    STRUCT(
      'station_SN' AS station_id,
      'Sachsen' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      26.0 AS temperature_celsius,
      48.0 AS humidity_percent,
      1009.0 AS pressure_hpa,
      6.0 AS wind_speed_ms,
      135.0 AS wind_direction_degrees,
      0.5 AS precipitation_mm,
      14.0 AS visibility_km,
      'cloudy' AS weather_condition,
      280.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Nordrhein-Westfalen (North Rhine-Westphalia) - Western Germany
    STRUCT(
      'station_NW' AS station_id,
      'Nordrhein-Westfalen' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      24.0 AS temperature_celsius,
      58.0 AS humidity_percent,
      1011.0 AS pressure_hpa,
      7.0 AS wind_speed_ms,
      270.0 AS wind_direction_degrees,
      1.0 AS precipitation_mm,
      16.0 AS visibility_km,
      'rainy' AS weather_condition,
      180.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Hessen (Hesse) - Central Germany
    STRUCT(
      'station_HE' AS station_id,
      'Hessen' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      25.5 AS temperature_celsius,
      52.0 AS humidity_percent,
      1010.5 AS pressure_hpa,
      9.0 AS wind_speed_ms,
      200.0 AS wind_direction_degrees,
      0.2 AS precipitation_mm,
      13.0 AS visibility_km,
      'cloudy' AS weather_condition,
      320.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Niedersachsen (Lower Saxony) - Northern Germany
    STRUCT(
      'station_NI' AS station_id,
      'Niedersachsen' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      23.0 AS temperature_celsius,
      65.0 AS humidity_percent,
      1013.0 AS pressure_hpa,
      11.0 AS wind_speed_ms,
      315.0 AS wind_direction_degrees,
      0.8 AS precipitation_mm,
      11.0 AS visibility_km,
      'windy' AS weather_condition,
      95.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Schleswig-Holstein - Northern coastal state
    STRUCT(
      'station_SH' AS station_id,
      'Schleswig-Holstein' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      21.0 AS temperature_celsius,
      72.0 AS humidity_percent,
      1014.0 AS pressure_hpa,
      13.0 AS wind_speed_ms,
      300.0 AS wind_direction_degrees,
      1.5 AS precipitation_mm,
      9.0 AS visibility_km,
      'rainy' AS weather_condition,
      45.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Mecklenburg-Vorpommern - Northeastern coast
    STRUCT(
      'station_MV' AS station_id,
      'Mecklenburg-Vorpommern' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      22.5 AS temperature_celsius,
      68.0 AS humidity_percent,
      1013.5 AS pressure_hpa,
      10.0 AS wind_speed_ms,
      45.0 AS wind_direction_degrees,
      0.3 AS precipitation_mm,
      10.0 AS visibility_km,
      'windy' AS weather_condition,
      70.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Berlin - Capital city state
    STRUCT(
      'station_BE' AS station_id,
      'Berlin' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      26.5 AS temperature_celsius,
      45.0 AS humidity_percent,
      1009.5 AS pressure_hpa,
      7.5 AS wind_speed_ms,
      120.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      13.5 AS visibility_km,
      'sunny' AS weather_condition,
      55.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Hamburg - Northern port city
    STRUCT(
      'station_HH' AS station_id,
      'Hamburg' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      22.0 AS temperature_celsius,
      70.0 AS humidity_percent,
      1014.5 AS pressure_hpa,
      12.0 AS wind_speed_ms,
      280.0 AS wind_direction_degrees,
      2.0 AS precipitation_mm,
      8.0 AS visibility_km,
      'rainy' AS weather_condition,
      25.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Bremen - Northwestern city state
    STRUCT(
      'station_HB' AS station_id,
      'Bremen' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      23.5 AS temperature_celsius,
      66.0 AS humidity_percent,
      1013.8 AS pressure_hpa,
      11.5 AS wind_speed_ms,
      295.0 AS wind_direction_degrees,
      1.2 AS precipitation_mm,
      9.5 AS visibility_km,
      'windy' AS weather_condition,
      15.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Rheinland-Pfalz (Rhineland-Palatinate) - Southwest
    STRUCT(
      'station_RP' AS station_id,
      'Rheinland-Pfalz' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      27.5 AS temperature_celsius,
      46.0 AS humidity_percent,
      1011.2 AS pressure_hpa,
      8.5 AS wind_speed_ms,
      210.0 AS wind_direction_degrees,
      0.1 AS precipitation_mm,
      14.5 AS visibility_km,
      'sunny' AS weather_condition,
      250.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Saarland - Small southwestern state
    STRUCT(
      'station_SL' AS station_id,
      'Saarland' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      28.0 AS temperature_celsius,
      44.0 AS humidity_percent,
      1010.8 AS pressure_hpa,
      9.5 AS wind_speed_ms,
      195.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      16.5 AS visibility_km,
      'sunny' AS weather_condition,
      290.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Sachsen-Anhalt (Saxony-Anhalt) - Central-eastern Germany
    STRUCT(
      'station_ST' AS station_id,
      'Sachsen-Anhalt' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      26.8 AS temperature_celsius,
      43.0 AS humidity_percent,
      1008.5 AS pressure_hpa,
      7.0 AS wind_speed_ms,
      150.0 AS wind_direction_degrees,
      0.0 AS precipitation_mm,
      12.5 AS visibility_km,
      'cloudy' AS weather_condition,
      120.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
    ),
    -- Thüringen (Thuringia) - Central Germany
    STRUCT(
      'station_TH' AS station_id,
      'Thüringen' AS state_province,
      DATETIME('2025-09-18 10:00:00', 'Europe/Berlin') AS timestamp,
      25.2 AS temperature_celsius,
      49.0 AS humidity_percent,
      1009.8 AS pressure_hpa,
      6.5 AS wind_speed_ms,
      170.0 AS wind_direction_degrees,
      0.2 AS precipitation_mm,
      13.8 AS visibility_km,
      'cloudy' AS weather_condition,
      380.0 AS elevation_meters,
      EXTRACT(MONTH FROM DATETIME('2025-09-18 10:00:00', 'Europe/Berlin')) AS month
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