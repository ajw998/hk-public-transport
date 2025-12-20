from __future__ import annotations

import os
from pathlib import Path

import httpx
import structlog
from hk_public_transport_etl.core import (
    atomic_write_text,
    relpath_posix,
    safe_unlink,
    sha256_file,
    utc_now_iso,
)
from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.registry import EndpointSpec, SourceSpec
from hk_public_transport_etl.stages.fetch.filename import resolve_artifact_filename
from hk_public_transport_etl.stages.fetch.http import (
    HttpFetchError,
    HttpResponseInfo,
    HttpStatusError,
    make_http_client,
    stream_get_to_file_with_retries,
)
from hk_public_transport_etl.stages.fetch.models import (
    RawArtifact,
    RawFetchResult,
    RawMetadata,
    RawMetadataArtifact,
)

log = structlog.get_logger(__name__)


class CacheCorruptionError(RuntimeError):
    pass


class StageFetchError(RuntimeError):
    pass


def _handle_not_modified(
    *,
    existing: RawMetadataArtifact,
    info: HttpResponseInfo,
    retrieved_at: str,
    version_root: Path,
    meta: RawMetadata,
    meta_path: Path,
) -> RawArtifact:
    entry = existing.model_copy(
        update={
            "final_url": info.final_url or existing.final_url,
            "retrieved_at_utc": retrieved_at,
            "status_code": 304,
            "etag": info.etag or existing.etag,
            "last_modified": info.last_modified or existing.last_modified,
            "content_type": info.content_type or existing.content_type,
            "cache_control": info.cache_control or existing.cache_control,
        }
    )
    meta.upsert_artifact(entry)
    meta.clear_error(existing.endpoint_id)
    _write_meta_atomic(meta_path, meta)

    p = version_root / Path(entry.path)
    return RawArtifact(
        endpoint_id=existing.endpoint_id,
        uri=entry.uri,
        path=str(p),
        sha256=entry.sha256,
        bytes=entry.bytes,
        etag=entry.etag,
        last_modified=entry.last_modified,
        retrieved_at_utc=entry.retrieved_at_utc,
    )


def _persist_successful_download(
    *,
    endpoint: EndpointSpec,
    source_id: str,
    uri: str,
    info: HttpResponseInfo,
    digest,
    retrieved_at: str,
    existing: RawMetadataArtifact | None,
    version_root: Path,
    artifacts_dir: Path,
    tmp_path: Path,
    used_names: set[str],
    meta: RawMetadata,
    meta_path: Path,
    force: bool,
) -> RawArtifact:
    # pick final path (keep existing path if already present)
    if existing is not None:
        final_path = version_root / Path(existing.path)
    else:
        name = resolve_artifact_filename(
            endpoint=endpoint,
            uri=uri,
            response_headers={
                "Content-Type": info.content_type,
                "Content-Disposition": info.content_disposition,
            },
            used_names=used_names,
        )
        used_names.add(name)
        final_path = artifacts_dir / name

    # immutability rule (within source/version)
    if (
        existing is not None
        and (
            digest.sha256.lower() != existing.sha256.lower()
            or digest.bytes != existing.bytes
        )
        and not force
    ):
        safe_unlink(tmp_path)
        msg = (
            f"{source_id}/{endpoint.id}: immutability breach for version={meta.version}. "
            f"cached {existing.sha256}/{existing.bytes}, new {digest.sha256}/{digest.bytes} from {uri}. "
            f"bump version or use --force."
        )
        meta.set_error(endpoint.id, msg)
        _write_meta_atomic(meta_path, meta)
        raise StageFetchError(msg)

    # refuse overwrite unless force, but allow idempotent match
    if final_path.exists() and not force:
        ex_digest = sha256_file(final_path)
        safe_unlink(tmp_path)
        if (
            ex_digest.sha256.lower() != digest.sha256.lower()
            or ex_digest.bytes != digest.bytes
        ):
            msg = f"{source_id}/{endpoint.id}: refusing to overwrite {final_path} (use --force)"
            meta.set_error(endpoint.id, msg)
            _write_meta_atomic(meta_path, meta)
            raise StageFetchError(msg)
    else:
        # atomic move into place
        final_path.parent.mkdir(parents=True, exist_ok=True)
        Path(tmp_path).replace(final_path)

    entry = RawMetadataArtifact(
        endpoint_id=endpoint.id,
        uri=uri,
        final_url=info.final_url,
        retrieved_at_utc=retrieved_at,
        status_code=200,
        etag=info.etag,
        last_modified=info.last_modified,
        content_type=info.content_type,
        cache_control=info.cache_control,
        bytes=digest.bytes,
        sha256=digest.sha256,
        filename=final_path.name,
        path=relpath_posix(final_path, version_root),
    )
    meta.upsert_artifact(entry)
    meta.clear_error(endpoint.id)
    _write_meta_atomic(meta_path, meta)

    return RawArtifact(
        endpoint_id=endpoint.id,
        uri=uri,
        path=str(final_path),
        sha256=digest.sha256,
        bytes=digest.bytes,
        etag=info.etag,
        last_modified=info.last_modified,
        retrieved_at_utc=retrieved_at,
    )


def _ensure_raw_dirs(
    layout: DataLayout, source_id: str, version: str
) -> tuple[Path, Path, Path, Path]:
    version_root = layout.raw(source_id, version)
    artifacts_dir = layout.raw_artifacts(source_id, version)
    tmp_dir = version_root / ".tmp"
    meta_path = layout.raw_metadata_json(source_id, version)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return version_root, artifacts_dir, tmp_dir, meta_path


def _write_meta_atomic(meta_path: Path, meta: RawMetadata) -> None:
    meta.updated_at_utc = utc_now_iso()
    atomic_write_text(meta_path, meta.model_dump_json(indent=2), encoding="utf-8")


def _load_or_init_meta(meta_path: Path, *, source_id: str, version: str) -> RawMetadata:
    if meta_path.exists():
        try:
            return RawMetadata.model_validate_json(
                meta_path.read_text(encoding="utf-8")
            )
        except Exception as e:  # noqa: BLE001
            raise StageFetchError(f"Failed to parse raw metadata: {meta_path}") from e

    now = utc_now_iso()
    meta = RawMetadata(
        source_id=source_id,
        version=version,
        created_at_utc=now,
        updated_at_utc=now,
    )
    _write_meta_atomic(meta_path, meta)
    return meta


def _verify_cached_artifact(*, version_root: Path, a: RawMetadataArtifact) -> None:
    p = version_root / Path(a.path)
    if not p.exists():
        raise CacheCorruptionError(f"Missing cached artifact for {a.endpoint_id}: {p}")
    digest = sha256_file(p)
    if digest.sha256.lower() != a.sha256.lower() or digest.bytes != a.bytes:
        raise CacheCorruptionError(
            f"Corrupt cached artifact for {a.endpoint_id}: expected {a.sha256}/{a.bytes}, got {digest.sha256}/{digest.bytes}"
        )


def _conditional_headers(
    existing: RawMetadataArtifact | None, uri: str
) -> dict[str, str]:
    if existing is None or existing.uri != uri:
        return {}
    h: dict[str, str] = {}
    if existing.etag:
        h["If-None-Match"] = existing.etag
    if existing.last_modified:
        h["If-Modified-Since"] = existing.last_modified
    return h


def _prioritize_existing_uri(
    candidates: list[str], existing_uri: str | None
) -> list[str]:
    if not existing_uri or existing_uri not in candidates:
        return candidates
    return [existing_uri] + [u for u in candidates if u != existing_uri]


def _new_tmp_path(tmp_dir: Path, endpoint_id: str) -> Path:
    return (
        tmp_dir
        / f"{endpoint_id}.{os.getpid()}.{utc_now_iso().replace(':','').replace('-','')}.part"
    )


def fetch_source(
    *,
    spec: SourceSpec,
    version: str,
    layout: DataLayout,
    force: bool = False,
    client: httpx.Client | None = None,
    max_attempts: int = 3,
) -> RawFetchResult:
    """
    Fetch all endpoints for a source into:
      data/raw/{source_id}/{version}/artifacts/...
      data/raw/{source_id}/{version}/raw_metadata.json
    """
    source_id = spec.id
    version_root, artifacts_dir, tmp_dir, meta_path = _ensure_raw_dirs(
        layout, source_id, version
    )
    meta = _load_or_init_meta(meta_path, source_id=source_id, version=version)

    used_names = {a.filename for a in meta.artifacts}
    owns_client = client is None
    if client is None:
        client = make_http_client()

    out: list[RawArtifact] = []
    try:
        for endpoint in spec.endpoints:
            art = _fetch_endpoint(
                spec=spec,
                endpoint=endpoint,
                version_root=version_root,
                artifacts_dir=artifacts_dir,
                tmp_dir=tmp_dir,
                meta_path=meta_path,
                meta=meta,
                used_names=used_names,
                client=client,
                force=force,
                max_attempts=max_attempts,
            )
            if art is not None:
                out.append(art)

        return RawFetchResult(
            source_id=source_id,
            version=version,
            artifacts=tuple(out),
            raw_metadata_path=str(meta_path),
        )
    finally:
        if owns_client:
            client.close()


def _fetch_endpoint(
    *,
    spec: SourceSpec,
    endpoint: EndpointSpec,
    version_root: Path,
    artifacts_dir: Path,
    tmp_dir: Path,
    meta_path: Path,
    meta: RawMetadata,
    used_names: set[str],
    client: httpx.Client,
    force: bool,
    max_attempts: int,
) -> RawArtifact | None:
    source_id = spec.id
    existing = meta.get_artifact(endpoint.id)

    # verify cached bytes if present
    if existing is not None:
        try:
            _verify_cached_artifact(version_root=version_root, a=existing)
        except CacheCorruptionError as e:
            meta.set_error(endpoint.id, str(e))
            _write_meta_atomic(meta_path, meta)
            raise

    candidates = endpoint.resolved_url_candidates(spec.base_urls)
    candidates = _prioritize_existing_uri(
        candidates, existing.uri if existing else None
    )

    last_err: BaseException | None = None
    for uri in candidates:
        tmp_path = _new_tmp_path(tmp_dir, endpoint.id)
        headers = _conditional_headers(existing, uri)

        try:
            dl = stream_get_to_file_with_retries(
                client,
                url=uri,
                dest_path=tmp_path,
                headers=headers,
                allowed_statuses=(200, 304),
                max_attempts=max_attempts,
            )
        except (HttpStatusError, HttpFetchError) as e:
            last_err = e
            continue

        retrieved_at = utc_now_iso()

        if dl.info.status_code == 304:
            if existing is None:
                last_err = StageFetchError(
                    f"{source_id}/{endpoint.id}: 304 for {uri} but no cached artifact exists"
                )
                continue

            return _handle_not_modified(
                existing=existing,
                info=dl.info,
                retrieved_at=retrieved_at,
                version_root=version_root,
                meta=meta,
                meta_path=meta_path,
            )

        # 200
        digest = sha256_file(Path(tmp_path))
        if digest.bytes <= 0:
            safe_unlink(tmp_path)
            last_err = StageFetchError(
                f"{source_id}/{endpoint.id}: empty content from {uri}"
            )
            continue

        return _persist_successful_download(
            endpoint=endpoint,
            source_id=source_id,
            uri=uri,
            info=dl.info,
            digest=digest,
            retrieved_at=retrieved_at,
            existing=existing,
            version_root=version_root,
            artifacts_dir=artifacts_dir,
            tmp_path=Path(tmp_path),
            used_names=used_names,
            meta=meta,
            meta_path=meta_path,
            force=force,
        )

    # none succeeded
    msg = f"{source_id}/{endpoint.id}: fetch failed for all candidates: {last_err}"
    meta.set_error(endpoint.id, msg)
    _write_meta_atomic(meta_path, meta)

    if endpoint.required:
        raise StageFetchError(msg) from (
            last_err if isinstance(last_err, Exception) else None
        )

    log.warn(
        "endpoint.optional_failed",
        source_id=source_id,
        endpoint_id=endpoint.id,
        error=str(last_err),
    )
    return None
