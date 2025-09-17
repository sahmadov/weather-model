terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 7.2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.1"
    }
  }
  required_version = ">= 1.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "random" {
  # No configuration needed for random provider
}