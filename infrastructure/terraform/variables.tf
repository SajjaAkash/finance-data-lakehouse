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
