# AWS provider pointed at LocalStack, not real AWS.
#
# The four skip_* flags are what make `tofu plan` and `tofu validate` succeed with no LocalStack
# container running: the provider would otherwise try to reach the EC2 metadata service and the
# STS GetCallerIdentity endpoint at configure time, which do not exist offline. With them off the
# provider stays inert until a resource is actually applied, so CI can compute a full create plan
# from static test credentials alone.
#
# The endpoints block routes every AWS API call (s3, glue, iam, sts) to the single LocalStack
# gateway on :4566. s3_use_path_style is mandatory for LocalStack S3: it addresses buckets as
# host/bucket rather than bucket.host, which has no DNS entry locally.

provider "aws" {
  region     = var.region
  access_key = var.access_key
  secret_key = var.secret_key

  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true

  endpoints {
    s3   = var.localstack_endpoint
    glue = var.localstack_endpoint
    iam  = var.localstack_endpoint
    sts  = var.localstack_endpoint
  }
}
