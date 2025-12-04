# SSM Parameters for credentials
resource "random_password" "redshift_password" {
  length  = 16
  special = false
}

resource "aws_ssm_parameter" "redshift_db_password" {
  name  = "/dev/redshift/db_password"
  type  = "SecureString"
  value = random_password.redshift_password.result
}

data "aws_ssm_parameter" "redshift_db_username" {
  name = "/dev/redshift/db_username"
}

# Redshift Serverless Namespace
resource "aws_redshiftserverless_namespace" "tele_namespace" {
  namespace_name = "telecom-namespace"
  db_name        = "telecomdb"
  admin_username = data.aws_ssm_parameter.redshift_db_username.value
  admin_user_password = aws_ssm_parameter.redshift_db_password.value

  iam_roles = [
    aws_iam_role.redshift_role.arn
  ]

  tags = merge(local.generic_tag, local.redshift_tags)
}

# Redshift Serverless Workgroup
resource "aws_redshiftserverless_workgroup" "tele_workgroup" {
  workgroup_name = "telecom-workgroup"
  namespace_name = aws_redshiftserverless_namespace.tele_namespace.namespace_name
  base_capacity = 16
  publicly_accessible = true

  tags = merge(local.generic_tag, local.redshift_tags)
}