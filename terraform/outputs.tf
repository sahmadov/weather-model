output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.weather_dataset.dataset_id
}

output "dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.weather_dataset.location
}

output "bigquery_console_url" {
  description = "BigQuery console URL for the dataset"
  value       = "https://console.cloud.google.com/bigquery?project=${var.project_id}&ws=!1m4!1m3!3m2!1s${var.project_id}!2s${google_bigquery_dataset.weather_dataset.dataset_id}"
}

output "ml_model_name" {
  description = "Name of the ML model for wildfire prediction"
  value       = "${var.project_id}.${var.dataset_id}.${var.ml_model_name}"
}

output "training_data_table" {
  description = "Training data table name"
  value       = "${var.project_id}.${var.dataset_id}.wildfire_training_data"
}

output "predictions_table" {
  description = "Predictions table name"
  value       = "${var.project_id}.${var.dataset_id}.next_day_wildfire_predictions"
}

output "next_steps" {
  description = "Next steps after deployment"
  value = [
    "1. View your data in BigQuery: ${google_bigquery_dataset.weather_dataset.dataset_id}",
    "2. Check model performance with: SELECT * FROM ML.EVALUATE(MODEL `${var.project_id}.${var.dataset_id}.${var.ml_model_name}`)",
    "3. View predictions in: next_day_wildfire_predictions table",
    "4. Generate more predictions by running the ML.PREDICT function"
  ]
}
