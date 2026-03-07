.PHONY: sync migrate api worker plan-validator smoke

sync:
	uv sync || poetry install

migrate:
	uv run alembic -c apps/orchestrator_api/alembic.ini upgrade head

api:
	uv run orchestrator-api

worker:
	uv run worker

plan-validator:
	uv run plan-validator-api

smoke:
	uv run smoke
