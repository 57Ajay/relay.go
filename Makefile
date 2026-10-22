SHELL := /bin/bash
BENCH_DATABASE_URL ?= postgres://relay:relay@localhost:5432/relay?sslmode=disable

.PHONY: build test lint integration up down reset logs bench

build:
	go build -o bin/ ./cmd/...

test:
	go vet ./...
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

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

bench:
	go run ./cmd/loadgen -rate $${RATE:-1000} -duration $${DURATION:-30s} -db "$(BENCH_DATABASE_URL)"
