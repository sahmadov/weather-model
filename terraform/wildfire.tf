# terraform/wildfire.tf
# Simple Wildfire Risk Model

# Create table for wildfire risk assessments
resource "google_bigquery_table" "wildfire_risk" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "wildfire_risk"
  project    = var.project_id

  deletion_protection = false
  description = "Daily wildfire risk assessments based on weather data"

  schema = jsonencode([
    {
      name = "station_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Weather station identifier"
    },
    {
      name = "date"
      type = "DATE"
      mode = "REQUIRED"
      description = "Date of risk assessment"
    },
    {
      name = "risk_score"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Risk score (0-100)"
    },
    {
      name = "risk_level"
      type = "STRING"
      mode = "REQUIRED"
      description = "Risk level: LOW, MODERATE, HIGH, EXTREME"
    },
    {
      name = "max_temperature"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Maximum temperature for the day"
    },
    {
      name = "min_humidity"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Minimum humidity for the day"
    },
    {
      name = "max_wind_speed"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Maximum wind speed for the day"
    },
    {
      name = "days_without_rain"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Number of consecutive days without significant rain"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["station_id", "risk_level"]
  labels = var.labels
}

# Create view for current wildfire risk
resource "google_bigquery_table" "current_wildfire_risk" {
  depends_on = [google_bigquery_table.wildfire_risk]

  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "current_wildfire_risk"
  project    = var.project_id

  description = "Current wildfire risk status by station"

  view {
    query = <<EOF
SELECT
  ws.station_name,
  ws.city,
  wr.date,
  wr.risk_level,
  wr.risk_score,
  wr.max_temperature,
  wr.min_humidity,
  wr.max_wind_speed,
  wr.days_without_rain
FROM `${var.project_id}.${var.dataset_id}.wildfire_risk` wr
JOIN `${var.project_id}.${var.dataset_id}.weather_stations` ws
  ON wr.station_id = ws.station_id
WHERE wr.date = (
  SELECT MAX(date)
  FROM `${var.project_id}.${var.dataset_id}.wildfire_risk`
  WHERE station_id = wr.station_id
)
ORDER BY wr.risk_score DESC
EOF
    use_legacy_sql = false
  }

  labels = var.labels
}

# Simple calculation view
resource "google_bigquery_table" "wildfire_risk_calculation" {
  depends_on = [google_bigquery_table.wildfire_risk]

  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "wildfire_risk_calculation"
  project    = var.project_id

  description = "Calculate wildfire risk from weather observations"

  view {
    query = <<EOF
WITH daily_weather AS (
  SELECT
    station_id,
    DATE(timestamp) as date,
    MAX(temperature_celsius) as max_temp,
    MIN(humidity_percent) as min_humidity,
    MAX(wind_speed_ms) as max_wind_speed,
    SUM(precipitation_mm) as daily_rain
  FROM `${var.project_id}.${var.dataset_id}.weather_observations`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY station_id, DATE(timestamp)
),
risk_calculation AS (
  SELECT
    station_id,
    date,
    max_temp,
    min_humidity,
    max_wind_speed,
    daily_rain,

    -- Simple risk scoring (0-100)
    CASE
      WHEN max_temp >= 30 AND min_humidity <= 30 AND max_wind_speed >= 8 THEN 80 + RAND() * 20
      WHEN max_temp >= 25 AND min_humidity <= 40 AND max_wind_speed >= 6 THEN 60 + RAND() * 20
      WHEN max_temp >= 20 AND min_humidity <= 50 AND max_wind_speed >= 4 THEN 40 + RAND() * 20
      WHEN max_temp >= 15 AND min_humidity <= 60 THEN 20 + RAND() * 20
      ELSE RAND() * 20
    END as risk_score,

    -- Count days without rain
    COUNTIF(daily_rain < 1) OVER (
      PARTITION BY station_id
      ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as days_without_rain

  FROM daily_weather
)
SELECT
  station_id,
  date,
  ROUND(risk_score, 1) as risk_score,
  CASE
    WHEN risk_score >= 70 THEN 'EXTREME'
    WHEN risk_score >= 50 THEN 'HIGH'
    WHEN risk_score >= 30 THEN 'MODERATE'
    ELSE 'LOW'
  END as risk_level,
  max_temp as max_temperature,
  min_humidity,
  max_wind_speed,
  days_without_rain
FROM risk_calculation
ORDER BY station_id, date DESC
EOF
    use_legacy_sql = false
  }

  labels = var.labels
}