variable "aws_region" {
  type        = string
  description = "AWS region for the platform."
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Prefix used for AWS resources."
  default     = "finance-lakehouse"
}

variable "environment" {
  type        = string
  description = "Deployment environment identifier."
  default     = "dev"
}

variable "glue_database_name" {
  type        = string
  description = "Glue catalog database for the lakehouse."
  default     = "finance_lakehouse"
}

variable "vpc_id" {
  type        = string
  description = "VPC identifier used by MWAA."
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnet ids used for MWAA."
}

variable "mwaa_environment_name" {
  type        = string
  description = "Name of the MWAA environment."
  default     = "finance-lakehouse-mwaa"
}

variable "mwaa_environment_class" {
  type        = string
  description = "MWAA environment class."
  default     = "mw1.small"
}

variable "mwaa_min_workers" {
  type        = number
  description = "Minimum MWAA worker count."
  default     = 1
}

variable "mwaa_max_workers" {
  type        = number
  description = "Maximum MWAA worker count."
  default     = 4
}

variable "tags" {
  type        = map(string)
  description = "Additional resource tags."
  default     = {}
}
