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
  version    = "12"
  size       = "db-s-1vcpu-2gb"
  region     = "nyc1"
  node_count = 1
}
