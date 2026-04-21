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

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw"
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed"
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results"
}
