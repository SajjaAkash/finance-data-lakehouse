output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

output "athena_results_bucket_name" {
  value = aws_s3_bucket.athena_results.bucket
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.artifacts.bucket
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}

output "glue_job_names" {
  value = {
    market_bronze = aws_glue_job.market_bronze.name
    market_silver = aws_glue_job.market_silver.name
    sec_bronze    = aws_glue_job.sec_bronze.name
    sec_silver    = aws_glue_job.sec_silver.name
  }
}

output "mwaa_environment_name" {
  value = aws_mwaa_environment.this.name
}

output "mwaa_arn" {
  value = aws_mwaa_environment.this.arn
}
