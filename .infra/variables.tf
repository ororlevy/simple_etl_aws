variable "type" {
  description = "The type of the operation"
  type        = string
}

variable "region" {
  description = "The AWS region"
}

variable "endpoint" {
  description = "The AWS endpoint"
}

variable "credentials_key" {
  description = "AWS access key - only needed for localstack"
}

variable "credentials_secret" {
  description = "AWS secret key - only needed for localstack"
}

variable "dumper_time_limit_milli" {
  description = "Time limit for dumper in milliseconds"
}

variable "dumper_size_limit_mb" {
  description = "Size limit for dumper in MB"
}

variable "downloader_url" {
  description = "URL for the downloader"
}

variable "bucket" {
  description = "S3 bucket name"
}
