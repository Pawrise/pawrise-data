.PHONY: lint format-check test cov check

lint:
	uv run ruff check .

format-check:
	-uv run ruff format --check .

test:
	uv run pytest -q

cov:
	uv run pytest -q --cov --cov-report=term-missing --cov-report=html

# Reproduit le CI (ruff + pytest) et ajoute la couverture (seuil dans pyproject.toml).
check: lint format-check cov
