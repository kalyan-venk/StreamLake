# Lakehouse storage as code (Terraform + LocalStack)

This module declares StreamLake's lakehouse storage as infrastructure as code and provisions it
against [LocalStack](https://www.localstack.cloud/), so the whole thing runs offline with no
cloud account and no cost.

## What it provisions

| Resource | Purpose |
|---|---|
| `aws_s3_bucket.warehouse` | The Iceberg warehouse bucket. Bronze, silver, gold, and the `silver.trips_quarantine` reject table all live as prefixes under `warehouse/`. |
| `aws_s3_bucket_versioning.warehouse` | Keeps prior Iceberg metadata pointers so a bad commit can be rolled back. |
| `aws_s3_bucket_ownership_controls.warehouse` | `BucketOwnerEnforced`, ACLs off, access via policy/IAM. |
| `aws_glue_catalog_database.iceberg` | Glue Data Catalog database that fronts the Iceberg tables. |
| `aws_iam_role.spark` | Role a Spark job would assume in production (mocked on LocalStack). |
| `aws_iam_policy.warehouse_rw` | Read/write on the bucket objects, list on the bucket. |
| `aws_iam_role_policy_attachment.spark_warehouse` | Binds the policy to the role. |

## Why the provider skip flags matter

The AWS provider in `providers.tf` sets `skip_credentials_validation`,
`skip_requesting_account_id`, and `skip_metadata_api_check`. Those keep the provider inert at
configure time, so `tofu validate` and `tofu plan` compute a full create plan from static test
credentials with no LocalStack container running. That is what makes the CI `infra-localstack`
job deterministic and green offline. `s3_use_path_style = true` is required for LocalStack S3,
which has no per-bucket DNS.

## Usage

From the repo root:

```bash
make localstack-up     # start the LocalStack container on :4566, wait for health
make tf-ls-init        # tofu init (installs the aws provider)
make tf-ls-apply       # tofu apply -auto-approve
make tf-ls-output      # show the outputs (warehouse_bucket, warehouse_uri, ...)
make batch-localstack  # run the medallion batch spine against the TF-provisioned S3 bucket
make tf-ls-destroy     # tofu destroy -auto-approve
make localstack-down   # stop the container, free port 4566
```

Spark never hardcodes the bucket. `scripts/localstack_env.sh` reads `warehouse_bucket` from
`tofu output` and exports `ICEBERG_WAREHOUSE`, the S3 endpoint, path-style flag, and credentials
before exec-ing the job.

## Real AWS

The same module runs against real AWS: drop the `endpoints` block (or point
`localstack_endpoint` at the real regional endpoints) and supply real credentials. LocalStack is
enough for the reproducible, offline story.
