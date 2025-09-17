CREATE OR REPLACE MODEL `${project_id}.${dataset_id}.${model_name}`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['wildfire_risk_label'],
  auto_class_weights=TRUE,
  data_split_method='RANDOM',
  data_split_eval_fraction=0.2,
  max_iterations=49,
  learn_rate_strategy='CONSTANT',
  learn_rate=0.1,
  l1_reg=0.01,
  l2_reg=0.01
) AS
SELECT
  -- Core weather features
  temperature_celsius,
  humidity_percent,
  pressure_hpa,
  wind_speed_ms,
  wind_direction_degrees,
  precipitation_mm,
  visibility_km,

  -- Derived risk features
  temp_risk_score,
  humidity_risk_score,

  -- Station characteristics
  elevation_meters,

  -- Categorical features
  weather_condition,
  state_province,
  month,

  -- Target label
  wildfire_risk_label

FROM `${project_id}.${dataset_id}.wildfire_training_data`;