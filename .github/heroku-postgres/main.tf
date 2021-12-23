terraform {
  required_providers {
    heroku = {
      source = "heroku/heroku"
      version = "~> 4.0"
    }
  }
}

//resource "heroku_app" "default" {
//  name = "edgedb-heroku-ci"
//  region = "us"
//
//  organization {
//    name = "edgedb"
//  }
//}

resource "heroku_addon" "database" {
//  app = heroku_app.default.name
  app = "edgedb-heroku-ci"
  plan = "heroku-postgresql:hobby-basic"
  config = {}
}

output "heroku_postgres_dsn" {
  value = heroku_addon.database.config_var_values.DATABASE_URL
  sensitive = true
}
