.PHONY: up down logs lint fmt test test-unit test-integration migrate shell deploy destroy infra-lint test-infra clean test-cov docker-build check-health

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/
	mypy src/

fmt:
	ruff check --fix src/ tests/
	ruff format src/ tests/

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v -m integration

migrate:
	alembic upgrade head

shell:
	docker compose exec consumer /bin/bash

# --- Infrastructure (Week 6) ---

deploy:
	python -m infra.deploy

destroy:
	python -m infra.destroy --confirm

infra-lint:
	ruff check infra/
	ruff format --check infra/
	mypy infra/

test-infra:
	pytest tests/unit/test_infra_*.py -v

# --- Utility ---

clean:
	rm -rf .venv __pycache__ .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

test-cov:
	pytest tests/ -v --cov=src --cov=infra --cov-report=html

docker-build:
	docker compose build producer processor consumer llm-analyzer api

check-health:
	@curl -sf http://localhost:8000/api/v1/health && echo "" || echo "API not reachable"
