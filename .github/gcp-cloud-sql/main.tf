variable "password" {}

provider "google" {
  region  = "us-east1"
}

resource "google_sql_database_instance" "default" {
  database_version    = "POSTGRES_13"
  deletion_protection = false

  settings {
    tier = "db-custom-1-3840"

    ip_configuration {
      authorized_networks {
        value = "0.0.0.0/0"
      }
    }
  }
}

resource "google_sql_user" "users" {
  instance        = google_sql_database_instance.default.name
  name            = "postgres"
  password        = var.password
  deletion_policy = "ABANDON"
}

output "db_instance_address" {
  value = google_sql_database_instance.default.public_ip_address
}
