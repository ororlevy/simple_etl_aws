GO    := go
GOFMT := gofmt
APP_NAME := bootstrap
# TYPE is out of the scope of the Makefile. could be extract/load

test: ; $(info $(M) Testing $(PKG)…) @
	$(GO) test ./...



build:
	GOOS=linux GOARCH=amd64 $(GO) build -o $(APP_NAME)
	chmod +x $(APP_NAME)
	mv $(APP_NAME) ../lambda/$(TYPE)/

localstack-up:
	cd ../docker && docker compose up -d

localstack-down:
	cd ../docker && docker compose down

run-lambda:
	awslocal lambda invoke --function-name $(TYPE) output.txt
	cat output.txt

build-extract:
	TYPE=extract $(MAKE) build

run-extract:
	TYPE=extract $(MAKE) run-lambda
