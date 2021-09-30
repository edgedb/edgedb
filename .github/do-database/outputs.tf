output "db_instance_address" {
  value = digitalocean_database_cluster.default.host
}

output "db_instance_port" {
  value = digitalocean_database_cluster.default.port
}

output "db_instance_user" {
  value = digitalocean_database_cluster.default.user
}

output "db_instance_password" {
  value = digitalocean_database_cluster.default.password
  sensitive = true
}

output "db_instance_database" {
  value = digitalocean_database_cluster.default.database
}
