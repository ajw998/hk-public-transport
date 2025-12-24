# Hong Kong Public Transport

**Not affiliated with the HKSAR Government or transport operators. Built for public-good use.**

This monorepo ingests Hong Kong transport open data from the HKSAR Government Data Portal ([data.gov.hk](https://data.gov.hk/en/)), normalizes it into a canonical schema, and publishes versioned SQLite artefact for downstream consumption.

## Design Notes
We distinguish between a full, canonical artefact, and a purpose-built bundle for applications.
- `transport.sqlite`: Canonical truth model used by the pipeline for validation and downstream data science style queries
- `app.sqlite`: Serving model built from canonical, compact and indexed for fast in-app browsing and search

## Running the pipeline
Prerequisites:
- Python 3.12+
- `uv`

```bash
# Execute full pipeline
uv run python -m hk_public_transport_etl.cli run

# You can also run specific pipeline stages (fetch, parse, normalize, validate, commit, serve, publish)
uv run python -m hk_public_transport_etl.cli normalize
uv run python -m hk_public_transport_etl.cli serve

# Control headway inclusion
# Full (default): all headway tables
# Partial: pattern_headways and service_exceptions
# None: drop headway tables
uv run python -m hk_public_transport_etl.cli run --headway partial
```

Outputs are placed under `data/` (override via `HK_PUBLIC_TRANSPORT_DATA_ROOT`). 

## Commands
- `make format`: Run formatter

- `make format-check`:  Check formats

- `make smoke_check`: Run golden smoke checks against the latest published `app.sqlite`

- `make search_prefix Q=9`: Route-number prefix search (available filters: `MODE_ID=...`, `OPERATOR_ID=...`)

- `make search_fts Q='central*'`: Full-text search across route endpoints and stop names

## Architecture
```mermaid
flowchart TD
  Contracts["`**contracts** 
  DDL, schema version, hashes`"]
  Fetch["`**Fetch**
   raw artifacts`"]
  Parse["`**Parse**
  staged parquet`"]
  Normalize["`**Normalize**
  canonical mappings`"]
  Validate["`**Validate**
  checks and report`"]
  Commit["`**Commit**
  SQLite bundle`"]
  Publish["`**Publish** 
  Manifest and checksums and optimized SQLite`"]
  Serve["`**Serve**
  app.sqlite serving bundle`"]

  Contracts --> Normalize
  Contracts --> Validate

  Fetch --> Parse --> Normalize --> Validate --> Commit --> Serve --> Publish
```

## License
MIT

### Data License

Refer to the Hong Kong DATA.GOV.HK [Terms & Condition](https://data.gov.hk/en/terms-and-conditions).
