.PHONY: format format-check

format:
	uv run isort .
	uv run black .

format-check:
	uv run isort --check-only --diff .
	uv run black --check --diff .
