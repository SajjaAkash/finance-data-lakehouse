terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Workload    = "finance-lakehouse"
    },
    var.tags
  )

  scripts_prefix = "artifacts/glue"
  mwaa_logs_path = "airflow-logs"
}

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-${var.environment}-raw"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-${var.environment}-processed"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-${var.environment}-athena-results"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.project_name}-${var.environment}-artifacts"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "versioning" {
  for_each = {
    raw            = aws_s3_bucket.raw.id
    processed      = aws_s3_bucket.processed.id
    athena_results = aws_s3_bucket.athena_results.id
    artifacts      = aws_s3_bucket.artifacts.id
  }

  bucket = each.value
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  for_each = {
    raw            = aws_s3_bucket.raw.bucket
    processed      = aws_s3_bucket.processed.bucket
    athena_results = aws_s3_bucket.athena_results.bucket
    artifacts      = aws_s3_bucket.artifacts.bucket
  }

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "transition-older-delta-checkpoints"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_glue_catalog_database" "lakehouse" {
  name = var.glue_database_name
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-${var.environment}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "glue_access" {
  statement {
    sid = "LakehouseS3Access"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.processed.arn,
      "${aws_s3_bucket.processed.arn}/*",
      aws_s3_bucket.artifacts.arn,
      "${aws_s3_bucket.artifacts.arn}/*",
      aws_s3_bucket.athena_results.arn,
      "${aws_s3_bucket.athena_results.arn}/*",
    ]
  }

  statement {
    sid = "GlueCatalogAccess"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
    ]
    resources = ["*"]
  }

  statement {
    sid = "CloudWatchLogsAccess"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_access" {
  name   = "${var.project_name}-${var.environment}-glue-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_access.json
}

data "aws_iam_policy_document" "mwaa_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa" {
  name               = "${var.project_name}-${var.environment}-mwaa-role"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "mwaa_access" {
  statement {
    sid = "AirflowS3Access"
    actions = [
      "s3:GetObject*",
      "s3:GetBucket*",
      "s3:List*",
    ]
    resources = [
      aws_s3_bucket.artifacts.arn,
      "${aws_s3_bucket.artifacts.arn}/*",
    ]
  }

  statement {
    sid = "AirflowGlueControl"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }

  statement {
    sid = "AirflowLogsAndMetrics"
    actions = [
      "logs:CreateLogStream",
      "logs:CreateLogGroup",
      "logs:PutLogEvents",
      "cloudwatch:PutMetricData",
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "mwaa_access" {
  name   = "${var.project_name}-${var.environment}-mwaa-access"
  role   = aws_iam_role.mwaa.id
  policy = data.aws_iam_policy_document.mwaa_access.json
}

resource "aws_cloudwatch_log_group" "mwaa" {
  name              = "/aws/mwaa/${var.mwaa_environment_name}"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_security_group" "mwaa" {
  name        = "${var.project_name}-${var.environment}-mwaa-sg"
  description = "Security group for MWAA environment"
  vpc_id      = var.vpc_id
  tags        = local.common_tags

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_s3_object" "dag" {
  bucket       = aws_s3_bucket.artifacts.bucket
  key          = "dags/finance_lakehouse_dag.py"
  source       = "${path.module}/../../dags/finance_lakehouse_dag.py"
  etag         = filemd5("${path.module}/../../dags/finance_lakehouse_dag.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "requirements" {
  bucket       = aws_s3_bucket.artifacts.bucket
  key          = "requirements/requirements-mwaa.txt"
  source       = "${path.module}/../../requirements-mwaa.txt"
  etag         = filemd5("${path.module}/../../requirements-mwaa.txt")
  content_type = "text/plain"
}

resource "aws_s3_object" "market_bronze_job" {
  bucket       = aws_s3_bucket.artifacts.bucket
  key          = "${local.scripts_prefix}/glue_market_bronze.py"
  source       = "${path.module}/../../src/finance_lakehouse/jobs/glue_market_bronze.py"
  etag         = filemd5("${path.module}/../../src/finance_lakehouse/jobs/glue_market_bronze.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "market_silver_job" {
  bucket       = aws_s3_bucket.artifacts.bucket
  key          = "${local.scripts_prefix}/glue_market_silver.py"
  source       = "${path.module}/../../src/finance_lakehouse/jobs/glue_market_silver.py"
  etag         = filemd5("${path.module}/../../src/finance_lakehouse/jobs/glue_market_silver.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "sec_bronze_job" {
  bucket       = aws_s3_bucket.artifacts.bucket
  key          = "${local.scripts_prefix}/glue_sec_bronze.py"
  source       = "${path.module}/../../src/finance_lakehouse/jobs/glue_sec_bronze.py"
  etag         = filemd5("${path.module}/../../src/finance_lakehouse/jobs/glue_sec_bronze.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "sec_silver_job" {
  bucket       = aws_s3_bucket.artifacts.bucket
  key          = "${local.scripts_prefix}/glue_sec_silver.py"
  source       = "${path.module}/../../src/finance_lakehouse/jobs/glue_sec_silver.py"
  etag         = filemd5("${path.module}/../../src/finance_lakehouse/jobs/glue_sec_silver.py")
  content_type = "text/x-python"
}

resource "aws_glue_job" "market_bronze" {
  name     = "${var.project_name}-${var.environment}-market-bronze"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.market_bronze_job.key}"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 1
  timeout           = 30

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--raw_bucket"                       = aws_s3_bucket.raw.bucket
    "--processed_bucket"                 = aws_s3_bucket.processed.bucket
    "--glue_database"                    = aws_glue_catalog_database.lakehouse.name
    "--source_name"                      = "market_data"
  }

  tags = local.common_tags
}

resource "aws_glue_job" "market_silver" {
  name     = "${var.project_name}-${var.environment}-market-silver"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.market_silver_job.key}"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 1
  timeout           = 30

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--raw_bucket"                       = aws_s3_bucket.raw.bucket
    "--processed_bucket"                 = aws_s3_bucket.processed.bucket
    "--glue_database"                    = aws_glue_catalog_database.lakehouse.name
    "--source_name"                      = "market_data"
  }

  tags = local.common_tags
}

resource "aws_glue_job" "sec_bronze" {
  name     = "${var.project_name}-${var.environment}-sec-bronze"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.sec_bronze_job.key}"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 1
  timeout           = 30

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--raw_bucket"                       = aws_s3_bucket.raw.bucket
    "--processed_bucket"                 = aws_s3_bucket.processed.bucket
    "--glue_database"                    = aws_glue_catalog_database.lakehouse.name
    "--source_name"                      = "sec_submissions"
  }

  tags = local.common_tags
}

resource "aws_glue_job" "sec_silver" {
  name     = "${var.project_name}-${var.environment}-sec-silver"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.sec_silver_job.key}"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 1
  timeout           = 30

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--raw_bucket"                       = aws_s3_bucket.raw.bucket
    "--processed_bucket"                 = aws_s3_bucket.processed.bucket
    "--glue_database"                    = aws_glue_catalog_database.lakehouse.name
    "--source_name"                      = "sec_submissions"
  }

  tags = local.common_tags
}

resource "aws_mwaa_environment" "this" {
  name               = var.mwaa_environment_name
  execution_role_arn = aws_iam_role.mwaa.arn
  source_bucket_arn  = aws_s3_bucket.artifacts.arn
  dag_s3_path        = "dags"
  requirements_s3_path = aws_s3_object.requirements.key
  plugins_s3_path      = null
  startup_script_s3_path = null
  airflow_version       = "2.10.1"
  environment_class     = var.mwaa_environment_class
  max_workers           = var.mwaa_max_workers
  min_workers           = var.mwaa_min_workers
  schedulers            = 2
  webserver_access_mode = "PUBLIC_ONLY"
  security_group_ids    = [aws_security_group.mwaa.id]
  subnet_ids            = var.private_subnet_ids

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = local.common_tags
}
