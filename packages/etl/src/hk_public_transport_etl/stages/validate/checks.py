from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from .config import ValidateConfig
from .specs import ColumnRef, ValidationSpec
from .types import IssueCode, IssueCodeLike, Severity, ValidationIssue
from .util import df_rows, take_samples


def _resolve_col(df: pl.DataFrame, ref: ColumnRef) -> str | None:
    if isinstance(ref, str):
        return ref if ref in df.columns else None
    for c in ref:
        if c in df.columns:
            return c
    return None


def _resolve_cols(
    df: pl.DataFrame, refs: tuple[ColumnRef, ...]
) -> tuple[list[str], list[ColumnRef]]:
    chosen: list[str] = []
    missing: list[ColumnRef] = []
    for r in refs:
        c = _resolve_col(df, r)
        if c is None:
            missing.append(r)
        else:
            chosen.append(c)
    return chosen, missing


def _issue(
    *,
    severity: Severity,
    code: IssueCodeLike,
    table: str,
    message: str,
    count: int,
    columns: list[str] | None = None,
    samples: list[dict] | None = None,
    source_hint: str | None = None,
) -> ValidationIssue:
    return ValidationIssue(
        severity=severity,
        code=code,
        table=table,
        message=message,
        count=int(count),
        columns=tuple(columns or []),
        samples=tuple(samples or []),
        source_hint=source_hint,
    )


def _check_uniqueness(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    dups = (
        df.group_by(cols)
        .agg(pl.len().alias("_n"))
        .filter(pl.col("_n") > 1)
        .select(cols)
    )
    return df.join(dups, on=cols, how="semi") if dups.height > 0 else df.head(0)


def _check_fk_missing(
    child: pl.DataFrame,
    child_col: str,
    parent: pl.DataFrame,
    parent_col: str,
    *,
    only_non_null: bool,
) -> pl.DataFrame:
    c = child.filter(pl.col(child_col).is_not_null()) if only_non_null else child
    parent_keys = parent.select(pl.col(parent_col)).unique()
    return c.join(parent_keys, left_on=child_col, right_on=parent_col, how="anti")


def _mapping_hint_for_key(
    mappings: dict[str, pl.DataFrame], key_col: str
) -> tuple[str, pl.DataFrame] | None:
    preferred: list[str] = []
    if key_col == "place_id":
        preferred = ["map_place_source", "map_places_source"]
    elif key_col == "route_id":
        preferred = ["map_route_source", "map_routes_source"]
    elif key_col == "pattern_id":
        preferred = ["map_pattern_source", "map_patterns_source"]

    for name in preferred:
        df = mappings.get(name)
        if df is not None and key_col in df.columns:
            return name, df
    return None


def _enrich_samples_with_mappings(
    *,
    missing_df: pl.DataFrame,
    mappings: dict[str, pl.DataFrame],
    join_keys: tuple[str, ...],
) -> tuple[pl.DataFrame, str | None]:
    enriched = missing_df
    hints: list[str] = []

    for key in join_keys:
        if key not in enriched.columns:
            continue
        found = _mapping_hint_for_key(mappings, key)
        if found is None:
            continue
        map_name, map_df = found

        cols: list[str] = []
        for c in ["source", "source_id", "source_sub_id", "source_id_type"]:
            if c in map_df.columns:
                cols.append(c)
        if not cols:
            continue

        reduced = (
            map_df.select([key] + cols)
            .sort([key] + [c for c in cols if c in map_df.columns])
            .group_by(key)
            .agg([pl.first(c).alias(f"_src_{c}") for c in cols])
        )
        enriched = enriched.join(reduced, on=key, how="left")
        hints.append(f"{map_name} on {key}")

    return (enriched, "enriched via " + "; ".join(hints)) if hints else (enriched, None)


@dataclass(slots=True)
class HardState:
    stop: bool = False
    stop_reason: str | None = None


@dataclass(slots=True)
class HardContext:
    spec: ValidationSpec
    tables: dict[str, pl.DataFrame]
    unresolved: dict[str, pl.DataFrame]
    mappings: dict[str, pl.DataFrame]
    cfg: ValidateConfig
    state: HardState = field(default_factory=HardState)


HardCheck = Callable[[HardContext], list[ValidationIssue]]


def _check_required_tables(ctx: HardContext) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    missing_required = [
        t for t in sorted(ctx.spec.required_tables) if t not in ctx.tables
    ]
    for t in missing_required:
        issues.append(
            _issue(
                severity=Severity.ERROR,
                code=IssueCode.TABLE_MISSING,
                table=t,
                message=f"Required canonical table '{t}' is missing for spec '{ctx.spec.spec_id}'.",
                count=1,
            )
        )

    if ctx.cfg.hard_stop_on_missing_core:
        missing_core = [t for t in sorted(ctx.spec.core_tables) if t not in ctx.tables]
        if missing_core:
            ctx.state.stop = True
            ctx.state.stop_reason = f"missing_core={missing_core}"
    return issues


def _check_schema_not_null_uniqueness(ctx: HardContext) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    for table_name, t_spec in ctx.spec.tables.items():
        df = ctx.tables.get(table_name)
        if df is None:
            continue

        _, missing_cols = _resolve_cols(df, t_spec.required_columns)
        if missing_cols:
            issues.append(
                _issue(
                    severity=Severity.ERROR,
                    code=IssueCode.SCHEMA_MISSING_COLUMNS,
                    table=table_name,
                    message=f"Table '{table_name}' is missing required columns (or aliases): {missing_cols}",
                    count=len(missing_cols),
                )
            )

        nn_cols, _ = _resolve_cols(df, t_spec.not_null_columns)
        for col in nn_cols:
            nnull = int(df.select(pl.col(col).null_count()).item())
            if nnull > 0:
                bad = df.filter(pl.col(col).is_null())
                sort_cols = [
                    c
                    for c in t_spec.sample_sort
                    if isinstance(c, str) and c in bad.columns
                ]
                issues.append(
                    _issue(
                        severity=Severity.ERROR,
                        code=IssueCode.KEY_COLUMN_NULL,
                        table=table_name,
                        message=f"Key column '{col}' in '{table_name}' contains NULLs.",
                        count=nnull,
                        columns=[col],
                        samples=take_samples(
                            bad,
                            cols=list(dict.fromkeys(sort_cols + [col])),
                            sort_cols=sort_cols,
                            n=ctx.cfg.sample_size,
                        ),
                    )
                )

        for u in t_spec.uniqueness:
            u_cols, missing_u = _resolve_cols(df, u.columns)
            if missing_u:
                continue
            dup = _check_uniqueness(df, u_cols)
            if dup.height > 0:
                sort_cols = [
                    c
                    for c in t_spec.sample_sort
                    if isinstance(c, str) and c in dup.columns
                ]
                # allow spec-provided code but default to UNIQUENESS_VIOLATION
                code: IssueCodeLike = (
                    getattr(u, "code", IssueCode.UNIQUENESS_VIOLATION)
                    or IssueCode.UNIQUENESS_VIOLATION
                )
                sev = (
                    Severity(u.severity) if isinstance(u.severity, str) else u.severity
                )
                issues.append(
                    _issue(
                        severity=sev,
                        code=code,
                        table=table_name,
                        message=u.message or f"Uniqueness violated on {u_cols}.",
                        count=df_rows(dup),
                        columns=u_cols,
                        samples=take_samples(
                            dup,
                            cols=list(dict.fromkeys(sort_cols + u_cols)),
                            sort_cols=sort_cols,
                            n=ctx.cfg.sample_size,
                        ),
                    )
                )
    return issues


def _check_foreign_keys(ctx: HardContext) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    for fk in ctx.spec.foreign_keys:
        child = ctx.tables.get(fk.child_table)
        parent = ctx.tables.get(fk.parent_table)
        if child is None or parent is None:
            continue
        child_col = _resolve_col(child, fk.child_col)
        parent_col = _resolve_col(parent, fk.parent_col)
        if child_col is None or parent_col is None:
            continue

        missing = _check_fk_missing(
            child, child_col, parent, parent_col, only_non_null=fk.only_check_non_null
        )
        if missing.height == 0:
            continue

        enriched, hint = _enrich_samples_with_mappings(
            missing_df=missing,
            mappings=ctx.mappings,
            join_keys=fk.hint_join_keys,
        )

        t_spec = ctx.spec.tables.get(fk.child_table)
        sort_cols: list[str] = []
        if t_spec is not None:
            sort_cols = [
                c
                for c in t_spec.sample_sort
                if isinstance(c, str) and c in enriched.columns
            ]
        if not sort_cols and child_col in enriched.columns:
            sort_cols = [child_col]

        sev = Severity(fk.severity) if isinstance(fk.severity, str) else fk.severity
        code: IssueCodeLike = (
            getattr(fk, "code", IssueCode.FK_MISSING) or IssueCode.FK_MISSING
        )

        issues.append(
            _issue(
                severity=sev,
                code=code,
                table=fk.child_table,
                message=fk.message
                or f"{fk.child_table}.{child_col} contains values not present in {fk.parent_table}.{parent_col}.",
                count=df_rows(missing),
                columns=[child_col],
                samples=take_samples(
                    enriched,
                    cols=list(
                        dict.fromkeys(
                            sort_cols
                            + [child_col]
                            + [c for c in enriched.columns if c.startswith("_src_")]
                        )
                    ),
                    sort_cols=sort_cols,
                    n=ctx.cfg.sample_size,
                ),
                source_hint=hint,
            )
        )

    return issues


def _check_pattern_stop_sequences(ctx: HardContext) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    ps = ctx.tables.get("pattern_stops")
    if ps is None or not all(c in ps.columns for c in ["pattern_id", "seq"]):
        return issues

    agg = (
        ps.group_by("pattern_id")
        .agg(
            pl.col("seq").min().alias("_min"),
            pl.col("seq").max().alias("_max"),
            pl.col("seq").n_unique().alias("_nuniq"),
            pl.len().alias("_nrows"),
        )
        .sort("pattern_id")
    )

    bad_base = agg.filter(pl.col("_min") != ctx.cfg.seq_base)
    if bad_base.height > 0:
        issues.append(
            _issue(
                severity=Severity.ERROR,
                code=IssueCode.PATTERN_SEQ_BASE_MISMATCH,
                table="pattern_stops",
                message=f"pattern_stops.seq must start at {ctx.cfg.seq_base} per pattern.",
                count=df_rows(bad_base),
                columns=["pattern_id", "seq"],
                samples=take_samples(
                    bad_base,
                    cols=["pattern_id", "_min", "_max", "_nrows"],
                    sort_cols=["pattern_id"],
                    n=ctx.cfg.sample_size,
                ),
            )
        )

    if ctx.cfg.require_contiguous_seq:
        bad_gap = agg.filter(pl.col("_nuniq") != (pl.col("_max") - pl.col("_min") + 1))
        if bad_gap.height > 0:
            issues.append(
                _issue(
                    severity=Severity.ERROR,
                    code=IssueCode.PATTERN_SEQ_GAPS_OR_DUPES,
                    table="pattern_stops",
                    message="pattern_stops.seq must be contiguous per pattern.",
                    count=df_rows(bad_gap),
                    columns=["pattern_id", "seq"],
                    samples=take_samples(
                        bad_gap,
                        cols=["pattern_id", "_min", "_max", "_nuniq", "_nrows"],
                        sort_cols=["pattern_id"],
                        n=ctx.cfg.sample_size,
                    ),
                )
            )

    return issues


def _check_unresolved_gating(ctx: HardContext) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    for name, u_spec in ctx.spec.unresolved.items():
        df = ctx.unresolved.get(name)
        if df is None:
            continue

        if name == "fare_orphans" and ctx.cfg.allow_unresolved_fares:
            continue

        if u_spec.fail_if_nonempty and df.height > 0:
            sort_cols = [c for c in u_spec.sample_sort if c in df.columns]
            issues.append(
                _issue(
                    severity=Severity.ERROR,
                    code=IssueCode.UNRESOLVED_NONEMPTY,
                    table=f"unresolved/{name}",
                    message=f"Unresolved table '{name}' is non-empty and is disallowed by spec '{ctx.spec.spec_id}'.",
                    count=df_rows(df),
                    columns=list(df.columns[: min(6, len(df.columns))]),
                    samples=take_samples(
                        df,
                        cols=list(df.columns[: min(6, len(df.columns))]),
                        sort_cols=sort_cols,
                        n=ctx.cfg.sample_size,
                    ),
                )
            )

    return issues


HARD_CHECKS: tuple[HardCheck, ...] = (
    _check_required_tables,
    _check_schema_not_null_uniqueness,
    _check_foreign_keys,
    _check_pattern_stop_sequences,
    _check_unresolved_gating,
)


def hard_validate(
    *,
    spec: ValidationSpec,
    tables: dict[str, pl.DataFrame],
    unresolved: dict[str, pl.DataFrame],
    mappings: dict[str, pl.DataFrame],
    cfg: ValidateConfig,
    checks: tuple[HardCheck, ...] = HARD_CHECKS,
) -> list[ValidationIssue]:
    ctx = HardContext(
        spec=spec, tables=tables, unresolved=unresolved, mappings=mappings, cfg=cfg
    )
    issues: list[ValidationIssue] = []

    for fn in checks:
        issues.extend(fn(ctx))
        if ctx.state.stop:
            break

    return issues


def soft_validate(
    *, tables: dict[str, pl.DataFrame], cfg: ValidateConfig
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    ps = tables.get("pattern_stops")
    if ps is not None and "pattern_id" in ps.columns:
        lens = ps.group_by("pattern_id").agg(pl.len().alias("_n")).sort("pattern_id")

        short = lens.filter(pl.col("_n") < cfg.min_pattern_stops_warn)
        if short.height > 0:
            issues.append(
                _issue(
                    severity=Severity.WARN,
                    code=IssueCode.PATTERN_TOO_SHORT,
                    table="pattern_stops",
                    message=f"Patterns with fewer than {cfg.min_pattern_stops_warn} stops detected.",
                    count=df_rows(short),
                    columns=["pattern_id"],
                    samples=take_samples(
                        short,
                        cols=["pattern_id", "_n"],
                        sort_cols=["pattern_id"],
                        n=cfg.sample_size,
                    ),
                )
            )

        long = lens.filter(pl.col("_n") > cfg.max_pattern_stops_warn)
        if long.height > 0:
            issues.append(
                _issue(
                    severity=Severity.WARN,
                    code=IssueCode.PATTERN_TOO_LONG,
                    table="pattern_stops",
                    message=f"Patterns with more than {cfg.max_pattern_stops_warn} stops detected.",
                    count=df_rows(long),
                    columns=["pattern_id"],
                    samples=take_samples(
                        long,
                        cols=["pattern_id", "_n"],
                        sort_cols=["pattern_id"],
                        n=cfg.sample_size,
                    ),
                )
            )

    routes = tables.get("routes")
    fare_rules = tables.get("fare_rules")
    if (
        routes is not None
        and fare_rules is not None
        and "route_id" in routes.columns
        and "route_id" in fare_rules.columns
    ):
        fare_routes = fare_rules.select("route_id").drop_nulls().unique()
        missing = routes.join(fare_routes, on="route_id", how="anti")
        if missing.height > 0:
            cols = [
                c
                for c in ["route_id", "route_key", "operator_id"]
                if c in missing.columns
            ]
            issues.append(
                _issue(
                    severity=Severity.WARN,
                    code=IssueCode.ROUTE_MISSING_FARES,
                    table="routes",
                    message="Routes with no fare_rules detected.",
                    count=df_rows(missing),
                    columns=["route_id"],
                    samples=take_samples(
                        missing, cols=cols, sort_cols=["route_id"], n=cfg.sample_size
                    ),
                )
            )

    return issues
