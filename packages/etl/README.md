# Hong Kong Public Transport ETL

Compile HK Transport data into publishable bundle.

## Pipeline Stages

`Fetch`: Download raw source files from registered endpoints. See `config/sources`.

`Parse`: Convert raw sources files into staged parquet tables.

`Normalize`: Transform staged tables into canonical parquet tables, mapping tables, and unresolved outputs.

`Validate`: Check canonical tables for schema, uniqueness, foreign keys and flag any mismatches and issues.

`Commmit`: Assemble validated tables into SQLite bundle (unoptimized).

`Publish`: Package bundle with manifest, checksums (optimized).
