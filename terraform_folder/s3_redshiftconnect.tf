# Bucket Policy - This connects S3 to Redshift IAM role
resource "aws_s3_bucket_policy" "redshift_access_policy" {
  bucket = aws_s3_bucket.spectrum_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "AllowRedshiftServerlessAccess"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.redshift_role.arn
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.spectrum_bucket.arn,
          "${aws_s3_bucket.spectrum_bucket.arn}/*"
        ]
      }
    ]
  })
}