.PHONY: up down test load-test local-up local-down migrate

up:
	docker compose up --build

down:
	docker compose down -v

migrate:
	cd backend && ../.venv/bin/python -m app.db.migrate

test:
	cd backend && ../.venv/bin/pytest tests -v

load-test:
	.venv/bin/python scripts/load_test.py

local-up:
	./scripts/run_local.sh start

local-down:
	./scripts/run_local.sh stop
