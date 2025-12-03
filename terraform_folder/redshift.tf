
locals {
  redshift_tags = {
    Service-Name = "Redshift-Serverless"
  }
}

#
# IAM ROLE FOR REDSHIFT SERVERLESS
#
resource "aws_iam_role" "redshift_role" {
  name = "redshift-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "redshift-serverless.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(local.generic_tag, local.redshift_tags)
}

data "aws_iam_policy_document" "redshift_role_policy" {
  statement {
    sid = "S3ReadAndWrite"
    actions = [
      "s3:*List*",
      "s3:*Get*",
      "s3:*Put*"
    ]

    resources = [
      "arn:aws:s3:::kafayat-project-staging-data-lake",
      "arn:aws:s3:::kafayat-project-staging-data-lake/*",
    ]
  }

  statement {
    sid = "GlueAccess"
    actions = ["glue:*"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "redshift_policy" {
  name   = "serverless-redshift-policy"
  policy = data.aws_iam_policy_document.redshift_role_policy.json
}

resource "aws_iam_role_policy_attachment" "redshift_role_policy_bind" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = aws_iam_policy.redshift_policy.arn
}

#
# PASSWORD MANAGEMENT
#
resource "random_password" "redshift_password" {
  length  = 16
  special = false
}

resource "aws_ssm_parameter" "redshift_db_password" {
  name  = "/dev/redshift/db_password"
  type  = "String"
  value = random_password.redshift_password.result
}

data "aws_ssm_parameter" "redshift_db_username" {
  name = "/dev/redshift/db_username"
}

#
# REDSHIFT SERVERLESS NAMESPACE
#
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

#
# REDSHIFT SERVERLESS WORKGROUP
#
resource "aws_redshiftserverless_workgroup" "tele_workgroup" {
  workgroup_name = "telecom-workgroup"
  namespace_name = aws_redshiftserverless_namespace.tele_namespace.namespace_name

  # Choose compute size
  base_capacity = 16   # 16 RPUs (you can choose 8, 16, 32, 48, 128)

  publicly_accessible = true

  tags = merge(local.generic_tag, local.redshift_tags)
}
