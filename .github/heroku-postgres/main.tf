terraform {
  required_providers {
    heroku = {
      source  = "heroku/heroku"
      version = "~> 4.0"
    }
  }
}

resource "heroku_addon" "database" {
  app  = "edgedb-heroku-ci"
  plan = "heroku-postgresql:mini"
  config = {
    version = "14"
  }
}

output "heroku_postgres_dsn" {
  value     = heroku_addon.database.config_var_values.DATABASE_URL
  sensitive = true
}
