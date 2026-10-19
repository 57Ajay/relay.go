.PHONY: up down reset

up:
	docker compose up -d --build --wait

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans
