terraform {
  required_providers {
    digitalocean = {
      source = "digitalocean/digitalocean"
      version = "2.6.0"
    }
  }
}

variable "do_token" {}

provider "digitalocean" {

  token = var.do_token
}

resource "digitalocean_database_cluster" "default" {
  name = "edbtest"
  engine     = "pg"
  version    = "13"
  size       = "db-s-4vcpu-8gb"
  region     = "nyc1"
  node_count = 1
}
