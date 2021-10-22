variable "vpc_id" {
  description = "VPC ID"
}

variable "sg_id" {
  description = "security group ID"
}

variable "password" {
  description = "password, provide through your ENV variables"
}

module "aurora" {
  source  = "terraform-aws-modules/rds-aurora/aws"
  version = "~> 5.0"

  name           = "aws-aurora-instance"
  engine         = "aurora-postgresql"
  engine_version = "12.8"
  instance_type = "db.r6g.large"


  vpc_id  = var.vpc_id
  db_subnet_group_name	= "default"

  replica_count           = 1
  create_security_group   = false
  vpc_security_group_ids  = [var.sg_id]


  storage_encrypted   = true
  apply_immediately   = true

  username                = "edbtest"
  password                = var.password
  create_random_password  = false

  enabled_cloudwatch_logs_exports = ["postgresql"]
  publicly_accessible = true
  skip_final_snapshot = true

  tags = {
    Environment = "dev"
    Terraform   = "true"
  }
}

output "rds_cluster_endpoint" {
  description = "The cluster endpoint"
  value       = module.aurora.rds_cluster_endpoint
}
