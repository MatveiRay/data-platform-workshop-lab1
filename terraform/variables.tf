variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region (for provider)"
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "BigQuery location"
  type        = string
  default     = "EU"
}

variable "raw_dataset_id" {
  type    = string
  default = "raw"
}

variable "dwh_dataset_id" {
  type    = string
  default = "dwh"
}
