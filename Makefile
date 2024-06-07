deploy:
	cd .infra && terraform -chdir=localstack apply -auto-approve
	cd .infra && terraform apply -auto-approve -var-file="vars/extract/localstack.tfvars"

init:
	cd .infra && terraform init
	cd .infra/localstack && terraform init

build-extract:
	cd go && $(MAKE) build-extract

run-extract:
	cd go && $(MAKE) run-extract

flow:
	$(MAKE) build-extract
	$(MAKE) init
	$(MAKE) deploy
	$(MAKE) run-extract