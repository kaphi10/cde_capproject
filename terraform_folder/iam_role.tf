locals {
  redshift_tags = {
    Service-Name = "Redshift-Serverless"
  }
}

# IAM Role for Redshift Serverless
resource "aws_iam_role" "redshift_role" {
  name = "redshift-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = [
            "redshift.amazonaws.com",
            "redshift-serverless.amazonaws.com"
          ]
        }
      }
    ]
  })

  tags = merge(local.generic_tag, local.redshift_tags)
}

data "aws_iam_policy_document" "redshift_role_policy" {
  statement {
    sid = "S3ReadAccess"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]

    resources = [
      aws_s3_bucket.spectrum_bucket.arn,
      "${aws_s3_bucket.spectrum_bucket.arn}/*",
    ]
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