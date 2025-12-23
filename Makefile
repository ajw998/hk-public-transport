.PHONY: format format-check

format:
	uv run isort .
	uv run black .

format-check:
	uv run isort --check-only --diff .
	uv run black --check --diff .

STAGE ?= run

run:
	uv run python3 -m hk_public_transport_etl.cli $(STAGE)

smoke_check:
	uv run python3 scripts/smoke_check.py
