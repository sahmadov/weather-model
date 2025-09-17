variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "europe-west3"
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
  type        = string
  default     = "weather_data"

  validation {
    condition     = can(regex("^[a-zA-Z0-9_]+$", var.dataset_id))
    error_message = "Dataset ID must contain only letters, numbers, and underscores."
  }
}

variable "bq_location" {
  description = "The BigQuery dataset location"
  type        = string
  default     = "europe-west3"

  validation {
    condition = contains([
      "US", "EU", "asia-east1", "asia-northeast1", "asia-southeast1",
      "australia-southeast1", "europe-north1", "europe-west1", "europe-west2",
      "europe-west3", "europe-west4", "europe-west6", "northamerica-northeast1",
      "southamerica-east1", "us-central1", "us-east1", "us-east4", "us-west1",
      "us-west2", "us-west3", "us-west4"
    ], var.bq_location)
    error_message = "BigQuery location must be a valid region or multi-region."
  }
}

variable "default_table_expiration_ms" {
  description = "Default table expiration in milliseconds (null for no expiration)"
  type        = number
  default     = null
}

variable "dataset_access" {
  description = "Access control for the dataset"
  type = list(object({
    role           = string
    user_by_email  = optional(string)
    group_by_email = optional(string)
    special_group  = optional(string)
  }))
  default = [
    {
      role          = "OWNER"
      special_group = "projectOwners"
    },
    {
      role          = "READER"
      special_group = "projectReaders"
    },
    {
      role          = "WRITER"
      special_group = "projectWriters"
    }
  ]
}

variable "dataset_viewers" {
  description = "List of members who should have BigQuery Data Viewer role on the dataset"
  type        = list(string)
  default     = []
}

variable "dataset_editors" {
  description = "List of members who should have BigQuery Data Editor role on the dataset"
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default = {
    environment = "dev"
    purpose     = "weather-analytics"
    managed-by  = "terraform"
  }
}