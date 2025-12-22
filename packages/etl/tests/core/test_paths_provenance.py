from __future__ import annotations

from pathlib import Path

from hk_public_transport_etl.core import errors, paths, provenance, time


def test_datalayout_paths_and_dirs(tmp_path: Path) -> None:
    layout = paths.DataLayout(root=tmp_path)
    p = layout.normalized("src", "v1")
    assert p == tmp_path / "normalized" / "src" / "v1"

    layout.ensure_dirs("src", "v1")
    assert (tmp_path / "normalized" / "src" / "v1").is_dir()
    assert (tmp_path / "out" / "src" / "v1").is_dir()


def test_stage_error_and_run_id() -> None:
    try:
        raise ValueError("boom")
    except Exception as exc:
        err = errors.stage_error_from_exc(exc)
    assert err.exc_type == "ValueError"
    assert "boom" in err.message
    assert "ValueError" in err.traceback

    rid1, rid2 = provenance.new_run_id(), provenance.new_run_id()
    assert rid1 != rid2 and len(rid1) == 32


def test_timer_records_duration() -> None:
    with provenance.Timer() as t:
        pass
    assert t.duration_ms is not None and t.duration_ms >= 0


def test_time_helpers_format() -> None:
    assert time.utc_now_iso().endswith("Z")
    assert len(time.today_version().split("-")) == 3
