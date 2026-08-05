#!/usr/bin/env bash
# Read the lakehouse storage coordinates from `tofu output` and export them for Spark, then exec
# the wrapped command. The bucket name is never hardcoded anywhere: it comes from the Terraform
# state via `tofu output -raw warehouse_bucket`, so renaming the bucket in one .tf variable is
# enough to move the whole pipeline.
#
#   ./scripts/localstack_env.sh make batch
#   ./scripts/localstack_env.sh env | grep ICEBERG_WAREHOUSE
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
TF_DIR="$ROOT/infra/terraform-localstack"

BUCKET="$(cd "$TF_DIR" && tofu output -raw warehouse_bucket)"
S3_ENDPOINT="$(cd "$TF_DIR" && tofu output -raw s3_endpoint)"
REGION="$(cd "$TF_DIR" && tofu output -raw region)"

export ICEBERG_WAREHOUSE="s3a://${BUCKET}/warehouse"
export AWS_S3_ENDPOINT="$S3_ENDPOINT"
export AWS_S3_PATH_STYLE="true"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

# Two jars, not one: iceberg-aws-bundle gives Iceberg's own S3FileIO (table data files) the AWS
# SDK v2 client; hadoop-aws gives Hadoop's generic FileSystem (which a `hadoop`-type Iceberg
# catalog still uses for namespace/table-directory operations, a separate client) its S3A
# handler. Skipping either produces a real failure against MinIO (verified while building the
# card-transaction rebuild: UnsupportedFileSystemException without hadoop-aws, then a 403 from
# the Hadoop-side client if only the Iceberg-side credentials are configured), see
# docs/RUNBOOK.md's MinIO section for the full explanation. Loading both via this env var keeps
# the default local-FS and CI unit-test runs lean; spark.py appends it to the resolved package
# list, and configures both clients' credentials, only when the var / AWS_S3_ENDPOINT are set.
export SPARK_EXTRA_PACKAGES="org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1"

exec "$@"
