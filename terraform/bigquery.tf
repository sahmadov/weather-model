# Create BigQuery dataset for weather data
resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id  = var.dataset_id
  project     = var.project_id
  location    = var.bq_location

  friendly_name   = "Weather Data"
  description     = "Dataset containing weather information and related analytics"

  # Dataset expires after 30 days of no activity (optional)
  default_table_expiration_ms = var.default_table_expiration_ms

  # Access control
  dynamic "access" {
    for_each = var.dataset_access
    content {
      role          = access.value.role
      user_by_email = lookup(access.value, "user_by_email", null)
      group_by_email = lookup(access.value, "group_by_email", null)
      special_group = lookup(access.value, "special_group", null)
    }
  }

  labels = var.labels
}

# Create tables for weather data
resource "google_bigquery_table" "weather_observations" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "weather_observations"
  project    = var.project_id

  description = "Raw weather observation data"

  schema = jsonencode([
    {
      name = "observation_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Unique identifier for the weather observation"
    },
    {
      name = "station_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Weather station identifier"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Timestamp of the observation"
    },
    {
      name = "location"
      type = "GEOGRAPHY"
      mode = "NULLABLE"
      description = "Geographic location of the observation"
    },
    {
      name = "temperature_celsius"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Temperature in Celsius"
    },
    {
      name = "humidity_percent"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Relative humidity percentage"
    },
    {
      name = "pressure_hpa"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Atmospheric pressure in hectopascals"
    },
    {
      name = "wind_speed_ms"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Wind speed in meters per second"
    },
    {
      name = "wind_direction_degrees"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Wind direction in degrees"
    },
    {
      name = "precipitation_mm"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Precipitation in millimeters"
    },
    {
      name = "visibility_km"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Visibility in kilometers"
    },
    {
      name = "weather_condition"
      type = "STRING"
      mode = "NULLABLE"
      description = "General weather condition (sunny, cloudy, rainy, etc.)"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["station_id", "weather_condition"]

  labels = var.labels
}

resource "google_bigquery_table" "weather_stations" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "weather_stations"
  project    = var.project_id

  description = "Weather station metadata"

  schema = jsonencode([
    {
      name = "station_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Unique identifier for the weather station"
    },
    {
      name = "station_name"
      type = "STRING"
      mode = "REQUIRED"
      description = "Name of the weather station"
    },
    {
      name = "location"
      type = "GEOGRAPHY"
      mode = "REQUIRED"
      description = "Geographic location of the station"
    },
    {
      name = "elevation_meters"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Elevation above sea level in meters"
    },
    {
      name = "country"
      type = "STRING"
      mode = "NULLABLE"
      description = "Country where the station is located"
    },
    {
      name = "state_province"
      type = "STRING"
      mode = "NULLABLE"
      description = "State or province where the station is located"
    },
    {
      name = "city"
      type = "STRING"
      mode = "NULLABLE"
      description = "City where the station is located"
    },
    {
      name = "active"
      type = "BOOLEAN"
      mode = "REQUIRED"
      description = "Whether the station is currently active"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "When the station record was created"
    },
    {
      name = "updated_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
      description = "When the station record was last updated"
    }
  ])

  clustering = ["country", "state_province"]

  labels = var.labels
}

# Create a view for daily weather summaries
resource "google_bigquery_table" "daily_weather_summary" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "daily_weather_summary"
  project    = var.project_id

  description = "Daily aggregated weather data"

  view {
    query = <<EOF
SELECT
  station_id,
  DATE(timestamp) as date,
  AVG(temperature_celsius) as avg_temperature_celsius,
  MIN(temperature_celsius) as min_temperature_celsius,
  MAX(temperature_celsius) as max_temperature_celsius,
  AVG(humidity_percent) as avg_humidity_percent,
  AVG(pressure_hpa) as avg_pressure_hpa,
  AVG(wind_speed_ms) as avg_wind_speed_ms,
  SUM(precipitation_mm) as total_precipitation_mm,
  COUNT(*) as observation_count
FROM
  `${var.project_id}.${var.dataset_id}.weather_observations`
GROUP BY
  station_id, DATE(timestamp)
EOF
    use_legacy_sql = false
  }

  labels = var.labels
}

# IAM binding for BigQuery dataset
resource "google_bigquery_dataset_iam_binding" "dataset_viewers" {
  count       = length(var.dataset_viewers) > 0 ? 1 : 0
  dataset_id  = google_bigquery_dataset.weather_dataset.dataset_id
  project     = var.project_id
  role        = "roles/bigquery.dataViewer"
  members     = var.dataset_viewers
}

resource "google_bigquery_dataset_iam_binding" "dataset_editors" {
  count       = length(var.dataset_editors) > 0 ? 1 : 0
  dataset_id  = google_bigquery_dataset.weather_dataset.dataset_id
  project     = var.project_id
  role        = "roles/bigquery.dataEditor"
  members     = var.dataset_editors
}