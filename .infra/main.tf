terraform {
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

locals {
  config_content = templatefile("${path.module}/config.tpl.yml", {
    type                   = var.type
    region                 = var.region
    endpoint               = var.endpoint
    credentials_key        = var.credentials_key
    credentials_secret     = var.credentials_secret
    dumper_time_limit_milli = var.dumper_time_limit_milli
    dumper_size_limit_mb   = var.dumper_size_limit_mb
    downloader_url         = var.downloader_url
    bucket                 = var.bucket
  })
}

resource "local_file" "config" {
  filename = "${path.module}/../resources/config-${var.type}.yaml"
  content  = local.config_content
}
