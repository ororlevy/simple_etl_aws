GO    := go
GOFMT := gofmt
APP_NAME := bootstrap
ZIP_NAME := function.zip
# TYPE is out of the scope of the Makefile. could be extract/load
LAMBDA_NAME := $(TYPE)
CONFIG_FILE := ../resources/config-$(TYPE).yaml

test: ; $(info $(M) Testing $(PKG)…) @
	$(GO) test ./...

config:
	cd ../.infra && terraform init
	cd ../.infra && terraform apply -auto-approve -var-file="vars/${TYPE}/localstack.tfvars"

build-zip:
	GOOS=linux GOARCH=amd64 $(GO) build -o $(APP_NAME)
	chmod +x $(APP_NAME)
	zip ${ZIP_NAME} $(APP_NAME) $(CONFIG_FILE)

localstack-up:
	cd ../docker && docker compose up -d

localstack-down:
	cd ../docker && docker compose down

submit-lambda:
	awslocal lambda create-function \
		--function-name $(LAMBDA_NAME) \
		--runtime provided.al2023 \
		--zip-file fileb://$(ZIP_NAME) \
		--handler $(APP_NAME) \
		--role arn:aws:iam::000000000000:role/lambda-role

run-lambda:
	awslocal lambda invoke \
		--function-name $(LAMBDA_NAME) output.txt
	cat output.txt

check-lambda:
	cd .. && ./lambda_status.sh $(LAMBDA_NAME)

deploy-local:
	$(MAKE) config
	$(MAKE) build-zip
	$(MAKE) localstack-up
	$(MAKE) submit-lambda
	$(MAKE) check-lambda
	$(MAKE) run-lambda



