.PHONY: setup run test lint check clean

setup:
	uv sync --all-groups
	uv run python scripts/download_delta_jars.py
	uv run python scripts/bootstrap_windows_hadoop.py

run:
	uv run saas-pipeline run --env dev --tenant all --start-date 2025-01-01 --end-date 2025-06-30

test:
	uv run pytest --ignore=tests/integration
	uv run pytest tests/integration

lint:
	uv run ruff check .

check: lint test

clean:
	uv run python scripts/clean_generated.py
