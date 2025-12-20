from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pyarrow as pa
from hk_public_transport_etl.core.errors import ParseError
from hk_public_transport_etl.stages.parse.common import (
    Artifact,
    ParseJob,
    discover_files,
    ordered_columns,
    parse_data_last_updated_csv,
    run_jobs,
    sort_table,
)

from .specs import MODE_PLAN_SPECS, XmlTablePlan, company_code_plan


# TODO: This can be abstracted away
def parse(artifacts_dir: Path) -> dict[str, pa.Table]:
    files = discover_files(artifacts_dir)
    artifacts = []
    for p in files:
        a = _classify(p)
        if a is not None:
            artifacts.append(a)

    jobs = (exec_job(a) for a in artifacts)
    return run_jobs(jobs=jobs)


def _classify(path: Path) -> Artifact | None:
    name_u = path.name.upper()

    if name_u == "DATA_LAST_UPDATED_DATE.CSV":
        return Artifact(kind="data_last_updated_csv", path=path)

    if name_u == "COMPANY_CODE.XML":
        return Artifact(kind="company_code_xml", path=path)

    if path.suffix.lower() != ".xml":
        return None

    parts = path.stem.split("_", 1)
    if len(parts) != 2:
        raise ParseError(f"Unrecognized TD XML artifact filename: {path.name}")

    prefix = parts[0].upper()
    mode = parts[1].lower()

    if prefix not in MODE_PLAN_SPECS:
        raise ParseError(
            f"Unrecognized TD XML artifact prefix={prefix!r} file={path.name}"
        )

    return Artifact(kind="mode_xml", path=path, prefix=prefix, mode=mode)


def exec_job(a: Artifact) -> ParseJob:
    if a.kind == "data_last_updated_csv":
        return ParseJob(
            table_name="td_data_last_updated",
            parse=lambda p=a.path: parse_data_last_updated_csv(
                p, table_name="td_data_last_updated"
            ),
        )

    if a.kind == "company_code_xml":
        plan = company_code_plan()
        return ParseJob(
            table_name=plan.table_name,
            parse=lambda plan=plan, p=a.path: _parse_xml_table(p, plan),
        )

    if a.kind == "mode_xml":
        assert a.prefix and a.mode
        spec = MODE_PLAN_SPECS[a.prefix]
        plan = XmlTablePlan(
            table_name=f"{spec.table_prefix}_{a.mode}",
            record_tag_hint=spec.record_tag_hint,
            required_fields=spec.required_fields,
            type_hints=spec.type_hints,
            stable_sort_keys=spec.stable_sort_keys,
            known_first=spec.known_first,
            extra_constant_cols={"mode": a.mode},
        )
        return ParseJob(
            table_name=plan.table_name,
            parse=lambda plan=plan, p=a.path: _parse_xml_table(p, plan),
        )

    raise ParseError(f"Unhandled artifact kind: {a.kind!r} path={a.path}")


def _parse_xml_table(xml_path: Path, plan: XmlTablePlan) -> pa.Table:
    record_tag = _detect_record_tag(xml_path, hint=plan.record_tag_hint)
    rows = list(_iter_xml_records(xml_path, record_tag=record_tag))

    if not rows:
        return _empty_table(plan)

    _validate_required_fields(plan=plan, source_file=xml_path.name, rows=rows)

    all_keys: set[str] = set()
    for _, kv in rows:
        all_keys.update(kv.keys())

    cols = ordered_columns(
        present=all_keys,
        known_first=plan.known_first,
        extra_constant_cols=plan.extra_constant_cols.keys(),
        include_trace_cols=True,
    )

    schema = pa.schema([pa.field(c, _dtype_for_col(c, plan.type_hints)) for c in cols])
    arrays = _materialize_columns(
        table_name=plan.table_name,
        source_file=xml_path.name,
        rows=rows,
        ordered_cols=cols,
        schema=schema,
        type_hints=plan.type_hints,
        extra_constant_cols=plan.extra_constant_cols,
    )
    table = pa.Table.from_arrays(arrays, schema=schema)

    sort_cols = [k for k in plan.stable_sort_keys if k in table.schema.names]
    for t in ("source_file", "source_row"):
        if t in table.schema.names and t not in sort_cols:
            sort_cols.append(t)

    return sort_table(table, sort_keys=[(k, "ascending") for k in sort_cols])


def _empty_table(plan: XmlTablePlan) -> pa.Table:
    cols = ordered_columns(
        present=plan.known_first,
        known_first=plan.known_first,
        extra_constant_cols=plan.extra_constant_cols.keys(),
        include_trace_cols=True,
    )
    schema = pa.schema([pa.field(c, _dtype_for_col(c, plan.type_hints)) for c in cols])
    arrays = [pa.array([], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _validate_required_fields(
    *, plan: XmlTablePlan, source_file: str, rows: list[Tuple[int, dict[str, str]]]
) -> None:
    for row_num, kv in rows:
        missing = [
            f for f in plan.required_fields if not (kv.get(f) and kv.get(f).strip())
        ]
        if missing:
            raise ParseError(
                f"[{plan.table_name}] missing required fields in {source_file} at source_row={row_num}: missing={missing}"
            )


def _materialize_columns(
    *,
    table_name: str,
    source_file: str,
    rows: list[Tuple[int, dict[str, str]]],
    ordered_cols: list[str],
    schema: pa.Schema,
    type_hints: dict[str, pa.DataType],
    extra_constant_cols: dict[str, str],
) -> list[pa.Array]:
    data: dict[str, list[object]] = {c: [] for c in ordered_cols}

    for row_num, kv in rows:
        for c in ordered_cols:
            if c == "source_file":
                data[c].append(source_file)
                continue
            if c == "source_row":
                data[c].append(row_num)
                continue
            if c in extra_constant_cols:
                data[c].append(extra_constant_cols[c])
                continue

            raw = kv.get(c)
            dtype = type_hints.get(c, pa.string())
            data[c].append(
                _cast_value(
                    table_name=table_name,
                    source_file=source_file,
                    source_row=row_num,
                    field=c,
                    raw=raw,
                    dtype=dtype,
                )
            )

    return [pa.array(data[c], type=schema.field(c).type) for c in schema.names]


def _detect_record_tag(xml_path: Path, *, hint: str) -> str:
    try:
        for _, elem in ET.iterparse(xml_path, events=("end",)):
            if elem.tag == hint and len(list(elem)) > 0:
                return hint
            if elem.tag != "dataroot" and len(list(elem)) > 0:
                return elem.tag
    except ET.ParseError as e:
        raise ParseError(f"XML parse error in {xml_path.name}: {e}") from e
    return hint


def _iter_xml_records(
    xml_path: Path, *, record_tag: str
) -> Iterator[Tuple[int, dict[str, str]]]:
    row = 0
    try:
        for _, elem in ET.iterparse(xml_path, events=("end",)):
            if elem.tag != record_tag:
                continue
            children = list(elem)
            if not children:
                continue
            row += 1
            kv = {child.tag: (child.text or "").strip() for child in children}
            yield row, kv
            elem.clear()
    except ET.ParseError as e:
        raise ParseError(f"XML parse error in {xml_path.name}: {e}") from e


def _cast_value(
    *,
    table_name: str,
    source_file: str,
    source_row: int,
    field: str,
    raw: Optional[str],
    dtype: pa.DataType,
):
    if raw is None or raw == "":
        return None
    try:
        if pa.types.is_string(dtype):
            return raw
        if pa.types.is_int32(dtype):
            return int(raw)
        if pa.types.is_float64(dtype):
            return float(raw)
        if pa.types.is_timestamp(dtype):
            s = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            return datetime.fromisoformat(s)
    except Exception as e:
        raise ParseError(
            f"[{table_name}] cannot parse field={field} dtype={dtype} in {source_file} at source_row={source_row}: raw={raw!r}"
        ) from e
    return raw


def _dtype_for_col(name: str, type_hints: dict[str, pa.DataType]) -> pa.DataType:
    if name == "source_row":
        return pa.int32()
    if name == "source_file":
        return pa.string()
    return type_hints.get(name, pa.string())
