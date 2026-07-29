output "warehouse_bucket" {
  description = "Name of the S3 bucket backing the Iceberg warehouse."
  value       = aws_s3_bucket.warehouse.bucket
}

output "warehouse_uri" {
  description = "Iceberg warehouse URI. Spark reads this via scripts/localstack_env.sh, never hardcoded."
  value       = local.warehouse_uri
}

output "glue_database" {
  description = "Glue Data Catalog database that fronts the Iceberg tables (empty if Glue disabled)."
  value       = var.enable_glue ? aws_glue_catalog_database.iceberg[0].name : ""
}

output "s3_endpoint" {
  description = "S3 endpoint Spark/S3FileIO should talk to (LocalStack gateway)."
  value       = var.localstack_endpoint
}

output "region" {
  description = "AWS region the resources live in."
  value       = var.region
}
