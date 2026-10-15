.PHONY: up down generate test

up:
	docker compose up --build

down:
	docker compose down -v

generate:
	docker compose --profile tools run --rm generator -total 20000 -rate 500

test:
	go test ./...
