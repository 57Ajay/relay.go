SHELL := /bin/bash

.PHONY: build test integration up down reset logs

build:
	go build -o bin/ ./cmd/...

test:
	go vet ./...
	go test -race -count=1 ./...

integration:
	docker compose up -d --wait kafka postgres
	go test -race -count=1 -tags integration -timeout 5m ./tests/integration/...

up:
	docker compose up -d --build --wait

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f api consumer
