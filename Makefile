.PHONY: up down generate load-test test

up:
	docker compose up --build

down:
	docker compose down -v

generate:
	docker compose --profile tools run --rm generator -total 20000 -rate 500

load-test:
	./scripts/load_test.sh 10000000

test:
	go test ./...
