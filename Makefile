.PHONY: format format-check run smoke_check search_fts search_prefix

UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
UV := UV_CACHE_DIR=$(UV_CACHE_DIR) uv

format:
	$(UV) run isort .
	$(UV) run black .

format-check:
	$(UV) run isort --check-only --diff .
	$(UV) run black --check --diff .

STAGE ?= run

run:
	$(UV) run python3 -m hk_public_transport_etl.cli $(STAGE) --headway none

smoke_check:
	$(UV) run python3 scripts/smoke_check.py

search_fts:
	$(UV) run python3 scripts/search_check.py fts --q "$(or $(Q),9)"

search_prefix:
	$(UV) run python3 scripts/search_check.py prefix --q "$(or $(Q),9)" --mode-id "$(MODE_ID)" --operator-id "$(OPERATOR_ID)"
