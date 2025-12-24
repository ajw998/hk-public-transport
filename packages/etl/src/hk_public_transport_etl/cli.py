from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast

from hk_public_transport_etl.core import (
    bind,
    configure_logging,
    get_logger,
    load_settings,
    new_run_id,
)
from hk_public_transport_etl.core.time import today_version
from hk_public_transport_etl.pipeline.runner import PipelineRunner, RunnerConfig
from hk_public_transport_etl.pipeline.stage import Stage, StageFn
from hk_public_transport_etl.stages import (
    stage_commit,
    stage_fetch,
    stage_normalize,
    stage_parse,
    stage_publish,
    stage_serve,
    stage_validate,
)
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


@dataclass(frozen=True, slots=True)
class _CommonArgs:
    cmd: str
    config_dir: str | None
    version: str
    sources: list[str] | None
    headway: str


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--config-dir",
        default=None,
        help=(
            "Directory containing registry.json and sources/*.json. "
            "If omitted: uses HK_PUBLIC_TRANSPORT_CONFIG_DIR or ./config (when running inside packages/etl/)."
        ),
    )
    p.add_argument("--version", default=today_version(), help="Bundle version")
    p.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Only run for this source_id (repeatable). If omitted, uses the registry list.",
    )
    p.add_argument(
        "--headway",
        choices=("full", "partial", "none"),
        default="full",
        help="Headway handling: full (default), partial (pattern_headways + service_exceptions only), none (drop headway tables).",
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hk-public-transport-etl")
    sub = p.add_subparsers(dest="cmd", required=True)

    commands: dict[str, str] = {
        "fetch": "Fetch raw artifacts into data/raw/{source}/{version}/",
        "parse": "Parse raw artifacts into staged parquet tables",
        "normalize": "Normalize staged tables into canonical, mappings, and unresolved tables",
        "validate": "Validate normalized tables",
        "commit": "Commit normalized tables into Sqlite bundle",
        "publish": "Publish artefacts",
        "run": "Run complete pipeline",
    }

    for cmd, help_text in commands.items():
        sp = sub.add_parser(cmd, help=help_text)
        _add_common_args(sp)

    return p


def _common(args: argparse.Namespace) -> _CommonArgs:
    return _CommonArgs(
        cmd=str(args.cmd),
        config_dir=(str(args.config_dir) if args.config_dir else None),
        version=str(args.version),
        sources=list(args.sources) if args.sources else None,
        headway=str(args.headway),
    )


_STAGE_FNS: dict[str, Callable[[Any], object]] = {
    "fetch": stage_fetch,
    "parse": stage_parse,
    "normalize": stage_normalize,
    "validate": stage_validate,
    "commit": stage_commit,
    "serve": stage_serve,
    "publish": stage_publish,
}

_PIPELINES: dict[str, tuple[str, ...]] = {
    "fetch": ("fetch",),
    "parse": ("parse",),
    "normalize": ("normalize",),
    "validate": ("validate",),
    "commit": ("commit",),
    "serve": ("serve",),
    "publish": ("publish",),
    "run": ("fetch", "parse", "normalize", "validate", "commit", "serve", "publish"),
}


def _normalize_stage_fn(fn: Callable[[Any], object]) -> StageFn:
    def _wrapped(ctx):
        return cast(dict[str, Any] | None, fn(ctx))

    return _wrapped


def _with_status(stage_id: str, fn: StageFn) -> StageFn:
    def _run_with_status(ctx):
        with console.status(f"[bold]{stage_id}[/]", spinner="dots"):
            return fn(ctx)

    return _run_with_status


def _build_stages(cmd: str) -> list[Stage]:
    stage_ids = _PIPELINES[cmd]
    return [
        PipelineRunner.fn(
            stage_id=sid,
            fn=_with_status(sid, _normalize_stage_fn(_STAGE_FNS[sid])),
        )
        for sid in stage_ids
    ]


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    common = _common(args)

    s = load_settings()
    configure_logging(level=s.log_level, fmt=s.log_format)
    log = get_logger("hk_public_transport_etl")

    run_id = new_run_id()
    bind(run_id=run_id, command=common.cmd, version=common.version)

    stages = _build_stages(common.cmd)

    runner = PipelineRunner(
        stages=stages, cfg=RunnerConfig(stop_on_failure=True), logger=log
    )

    meta: dict[str, object] = {
        "version": common.version,
        "config_dir": common.config_dir,
        "source_ids": common.sources,
        "headway_mode": common.headway,
    }

    console.print(
        Panel.fit(
            Text(
                f"hk-public-transport-etl - {common.cmd}\nrun_id={run_id}\nversion={common.version}",
                style="bold",
            ),
            title="Run",
        )
    )

    exit_code, report_path = runner.run(
        data_root=Path(s.data_root),
        run_root=Path(s.run_root),
        run_id=run_id,
        meta=meta,  # type: ignore[arg-type]
    )

    tbl = Table(title="Result", show_header=True, box=None)
    tbl.add_row(
        "status", "[green]ok[/green]" if exit_code == 0 else "[red]failed[/red]"
    )
    tbl.add_row("report", str(report_path))
    console.print(tbl)

    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
