resource "aws_db_instance" "default" {
  allocated_storage = 10
  engine = "postgres"
  engine_version = "12.8"
  instance_class = "db.m6g.large"
  name = "edbtest"
  username = "edbtest"
  password = var.password
  parameter_group_name = "default.postgres12"
  skip_final_snapshot = true
  auto_minor_version_upgrade = false
  publicly_accessible = true
  vpc_security_group_ids = [var.sg_id]
}
