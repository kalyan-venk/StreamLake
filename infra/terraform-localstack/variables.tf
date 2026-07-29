variable "localstack_endpoint" {
  description = "LocalStack gateway URL. All AWS APIs (s3, glue, iam, sts) are routed here."
  type        = string
  default     = "http://localhost:4566"
}

variable "region" {
  description = "AWS region. LocalStack ignores it functionally but the provider requires one."
  type        = string
  default     = "us-east-1"
}

variable "access_key" {
  description = "Static credential for LocalStack. Any non-empty value works; not a real secret."
  type        = string
  default     = "test"
}

variable "secret_key" {
  description = "Static credential for LocalStack. Any non-empty value works; not a real secret."
  type        = string
  default     = "test"
}

variable "bucket_name" {
  description = "Name of the S3 bucket that holds the Iceberg warehouse."
  type        = string
  default     = "streamlake-lakehouse"
}

variable "warehouse_prefix" {
  description = "Key prefix under the bucket where Iceberg writes bronze/silver/gold tables."
  type        = string
  default     = "warehouse"
}

variable "glue_database" {
  description = "Glue Data Catalog database that fronts the Iceberg tables."
  type        = string
  default     = "streamlake"
}

variable "enable_glue" {
  description = <<-EOT
    Whether to provision the Glue Data Catalog database. Default true keeps it in the CI
    `tofu plan` proof and in a real-AWS apply. Set false when applying against the community
    LocalStack image, which does not implement Glue (CreateDatabase returns HTTP 501, it is a
    LocalStack Pro feature). The Iceberg hadoop catalog stores metadata next to the data on S3,
    so Glue is not required for the medallion pipeline to run.
  EOT
  type        = bool
  default     = true
}
