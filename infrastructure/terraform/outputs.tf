output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

output "athena_results_bucket_name" {
  value = aws_s3_bucket.athena_results.bucket
}
