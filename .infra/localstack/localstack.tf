terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 2.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

resource "docker_image" "localstack" {
  name         = "localstack/localstack:latest"
  keep_locally = false
}

resource "docker_container" "localstack" {
  name  = "localstack"
  image = docker_image.localstack.name

  env = [
    "SERVICES=s3,lambda,logs,iam",
    "DEFAULT_REGION= eu-west-1",
    "DOCKER_HOST=unix:///var/run/docker.sock",
    "DEBUG=1",
    "LAMBDA_EXECUTOR=docker-reuse",
    "LAMBDA_REMOTE_DOCKER=false",
    "LAMBDA_REMOVE_CONTAINERS=false"
  ]

  ports {
    internal = 4566
    external = 4566
  }
  ports {
    internal = 4572
    external = 4572
  }

  volumes {
    host_path      = "/var/run/docker.sock"
    container_path = "/var/run/docker.sock"
  }
  #  volumes {
  #    host_path      = "${path.module}/../docker/localstack"
  #    container_path = "/var/lib/localstack"
  #  }

  start      = true
  depends_on = [docker_image.localstack]

  provisioner "local-exec" {
    command = <<-EOT
      sh -c 'while ! awslocal s3api list-buckets; do echo waiting for LocalStack to be ready...; sleep 5; done'
    EOT
  }

  provisioner "local-exec" {
    command = "docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${self.name} | tr -d '\n' > localstack_ip.txt"
  }

}

output "localstack_container_id" {
  value = docker_container.localstack.id
}

