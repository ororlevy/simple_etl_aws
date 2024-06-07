terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 2.0"
    }
  }
}

data "terraform_remote_state" "localstack" {
  backend = "local"
  config  = {
    path = "${path.module}/localstack/terraform.tfstate"
  }
}

provider "aws" {
  access_key          = "test"
  secret_key          = "test"
  region              = "eu-west-1"
  s3_force_path_style = true
  endpoints {
    apigateway     = "http://localhost:4566"
    cloudformation = "http://localhost:4566"
    cloudwatch     = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    es             = "http://localhost:4566"
    firehose       = "http://localhost:4566"
    iam            = "http://localhost:4566"
    kinesis        = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    route53        = "http://localhost:4566"
    redshift       = "http://localhost:4566"
    s3             = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
    ses            = "http://localhost:4566"
    sns            = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    ssm            = "http://localhost:4566"
    stepfunctions  = "http://localhost:4566"
    sts            = "http://localhost:4566"
  }
}

locals {
  localstack_ip = trimspace(file("${path.module}/localstack/localstack_ip.txt"))
  config_content = templatefile("${path.module}/config.tpl.yml", {
    type                    = var.type
    region                  = var.region
    endpoint                = "http://${local.localstack_ip}:4566"
    credentials_key         = var.credentials_key
    credentials_secret      = var.credentials_secret
    dumper_time_limit_milli = var.dumper_time_limit_milli
    dumper_size_limit_mb    = var.dumper_size_limit_mb
    downloader_url          = var.downloader_url
    bucket                  = var.bucket
  })
}

resource "aws_s3_bucket" "raw-data" {
  bucket        = var.bucket
  force_destroy = true
  depends_on    = [data.terraform_remote_state.localstack]
}

resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda_exec_role"

  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Action    = "sts:AssumeRole",
        Effect    = "Allow",
        Sid       = "",
        Principal = {
          Service = "lambda.amazonaws.com",
        },
      },
    ],
  })
}

resource "aws_iam_policy" "lambda_s3_policy" {
  name   = "lambda_s3_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.raw-data.bucket}/*",
          "arn:aws:s3:::${aws_s3_bucket.raw-data.bucket}"
        ]
      }
    ],
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_policy_attachment" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

resource "local_file" "config" {
  filename = "${path.module}/../lambda/${var.type}/resources/config.yaml"
  content  = local.config_content
}

resource "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/${var.type}"
  output_path = "${path.module}/../resources/lambda_package.zip"
  depends_on  = [local_file.config]
}

resource "aws_lambda_function" "extract" {
  function_name = "extract"
  filename      = archive_file.lambda_package.output_path
  handler       = "bootstrap"
  runtime       = "go1.x"
  role          = aws_iam_role.lambda_exec_role.arn
  timeout       = 300

  environment {
    variables = {
      CONFIG_PATH = "/var/task/resources",
      ENDPOINT    = "http://${local.localstack_ip}:4566"
    }
  }

  depends_on = [local_file.config, data.terraform_remote_state.localstack]
}
