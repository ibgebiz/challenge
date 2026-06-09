.PHONY: fmt lint test test-unit test-integration test-e2e build up down swagger tidy

fmt:
	gofumpt -w .
	goimports -w .

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

test:
	go test ./... -count=1

test-unit:
	go test ./internal/domain/... ./internal/usecase/... -count=1

test-integration:
	go test ./internal/adapter/... -count=1 -tags=integration

test-e2e:
	go test ./test/e2e/... -count=1 -tags=e2e -v

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/scheduler ./cmd/scheduler

up:
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

swagger:
	swag init -g cmd/api/main.go -o api/openapi
