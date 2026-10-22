SHELL := /bin/bash
IMAGE ?= relay:dev
CLUSTER ?= relay
BENCH_DATABASE_URL ?= postgres://relay:relay@localhost:5432/relay?sslmode=disable

.PHONY: build test lint integration up down reset logs bench kind-up kind-deploy kind-down

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

kind-up:
	kind create cluster --name $(CLUSTER) --config deploy/kubernetes/kind.yaml
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)

kind-deploy:
	kubectl apply -k deploy/kubernetes
	kubectl -n relay wait --for=condition=complete job/create-topics --timeout=300s
	kubectl -n relay rollout status deploy/api deploy/consumer --timeout=300s

kind-down:
	kind delete cluster --name $(CLUSTER)
