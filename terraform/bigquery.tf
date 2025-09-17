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
# Create synthetic wildfire training data table
resource "google_bigquery_job" "create_wildfire_training_data" {
  job_id     = "create_training_data_${formatdate("YYYYMMDD_hhmmss", timestamp())}_${random_id.job_suffix.hex}"
  project    = var.project_id
  location   = var.bq_location

  query {
    query = templatefile("${path.module}/sql/create_wildfire_training_data.sql", {
      project_id = var.project_id
      dataset_id = var.dataset_id
      training_data_size = var.training_data_size
    })
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_dataset.weather_dataset
  ]
}

# Create synthetic wildfire risk ML model
resource "google_bigquery_job" "create_wildfire_model" {
  job_id     = "create_model_${formatdate("YYYYMMDD_hhmmss", timestamp())}_${random_id.job_suffix.hex}"
  project    = var.project_id
  location   = var.bq_location

  query {
    query = templatefile("${path.module}/sql/create_wildfire_model.sql", {
      project_id = var.project_id
      dataset_id = var.dataset_id
      model_name = var.ml_model_name
    })
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_job.create_wildfire_training_data
  ]
}

# Generate next day wildfire predictions
resource "google_bigquery_job" "create_predictions" {
  job_id     = "create_predictions_${formatdate("YYYYMMDD_hhmmss", timestamp())}_${random_id.job_suffix.hex}"
  project    = var.project_id
  location   = var.bq_location

  query {
    query = templatefile("${path.module}/sql/create_predictions.sql", {
      project_id = var.project_id
      dataset_id = var.dataset_id
      model_name = var.ml_model_name
    })
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_job.create_wildfire_model
  ]
}

# Static random suffix for this deployment (won't change unless destroyed)
resource "random_id" "job_suffix" {
  byte_length = 2

  keepers = {
    # This ensures the random value changes only when we want it to
    dataset_id = var.dataset_id
  }
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