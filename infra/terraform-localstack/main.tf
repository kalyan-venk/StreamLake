# StreamLake lakehouse storage, declared as code and provisioned against LocalStack.
#
# What this codifies: the S3 bucket that backs the Iceberg warehouse (bronze, silver, gold, and
# the silver.transactions_quarantine reject table all live as prefixes under it), a Glue Data Catalog
# database for those tables, and a minimal IAM role plus read/write policy that a real Spark
# job would assume in production. On LocalStack the IAM is mocked, so it proves the declaration
# is valid without incurring cloud cost. The same module runs against real AWS by pointing
# localstack_endpoint at the real regional endpoints (or removing the endpoints block).

locals {
  warehouse_uri = "s3://${aws_s3_bucket.warehouse.bucket}/${var.warehouse_prefix}"

  tags = {
    project   = "streamlake"
    component = "lakehouse-storage"
    managed   = "terraform"
  }
}

# --- S3 warehouse bucket -------------------------------------------------------------------

resource "aws_s3_bucket" "warehouse" {
  bucket = var.bucket_name
  tags   = local.tags
}

# Versioning keeps prior Iceberg metadata pointer files, so a bad commit can be rolled back to a
# known-good snapshot rather than lost.
resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

# BucketOwnerEnforced disables ACLs entirely, which is the current S3 recommendation: access is
# governed by bucket policy and IAM, not per-object ACLs.
resource "aws_s3_bucket_ownership_controls" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# --- Glue Data Catalog ---------------------------------------------------------------------

resource "aws_glue_catalog_database" "iceberg" {
  count = var.enable_glue ? 1 : 0

  name        = var.glue_database
  description = "StreamLake Iceberg tables (bronze, silver, gold) registered in the Glue catalog."

  location_uri = local.warehouse_uri
}

# --- Minimal mocked IAM --------------------------------------------------------------------

resource "aws_iam_role" "spark" {
  name = "streamlake-spark"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_policy" "warehouse_rw" {
  name        = "streamlake-warehouse-rw"
  description = "Read/write on the StreamLake warehouse bucket and list on its parent."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListWarehouseBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.warehouse.arn]
      },
      {
        Sid      = "ReadWriteWarehouseObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.warehouse.arn}/*"]
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "spark_warehouse" {
  role       = aws_iam_role.spark.name
  policy_arn = aws_iam_policy.warehouse_rw.arn
}
