locals {
  service = {
    Service-Name = "Redshift-spectrum-demo"
  }
}


resource "aws_s3_bucket" "spectrum_bucket" {
  bucket = "kafayat-project-staging-data-lake"

  tags = merge(
    local.service,
    local.generic_tag
  )
}


resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.spectrum_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

