# Deployment Guide

## AWS Components

- S3 buckets for raw, processed, artifacts, and Athena results.
- Glue catalog database plus four Glue ETL jobs.
- IAM roles and inline policies for Glue and MWAA.
- MWAA environment configured to load DAGs and requirements from the artifacts bucket.

## Deploy

1. Copy `infrastructure/terraform/terraform.tfvars.example` to `terraform.tfvars`.
2. Populate `vpc_id` and `private_subnet_ids` for your AWS account.
3. Run:

   ```powershell
   cd infrastructure/terraform
   terraform init
   terraform plan
   terraform apply
   ```

## Artifact Promotion

- DAG file uploads to `s3://<artifacts-bucket>/dags/`.
- Glue job scripts upload to `s3://<artifacts-bucket>/artifacts/glue/`.
- MWAA requirements upload to `s3://<artifacts-bucket>/requirements/`.

## Operational Notes

- Glue jobs are configured on Glue 4.0 with Python 3.
- MWAA is intentionally parameterized for environment class and worker counts.
- S3 encryption and versioning are enabled across all platform buckets.
