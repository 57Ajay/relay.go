SHELL := /bin/bash

.PHONY: build test up down reset logs

build:
	go build -o bin/ ./cmd/...

test:
	go vet ./...
	go test -race -count=1 ./...

up:
	docker compose up -d --build --wait

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f api consumer
