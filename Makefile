SERVICE_NAME ?= moderation_service

.PHONY: install test build run lint

install:
	go mod download

test:
	go test ./... -race -count=1 -parallel 4

build:
	docker build -t $(SERVICE_NAME):local .

run:
	go run ./cmd/server

lint:
	golangci-lint run ./...
