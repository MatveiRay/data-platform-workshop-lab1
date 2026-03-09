terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "raw" {
  dataset_id  = var.raw_dataset_id
  location    = var.location
  description = "Сырые данные (clickstream/orders)"
}

resource "google_bigquery_dataset" "dwh" {
  dataset_id  = var.dwh_dataset_id
  location    = var.location
  description = "Витрины и семантический слой"
}
