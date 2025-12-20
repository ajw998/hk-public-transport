from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import httpx
import structlog
from hk_public_transport_etl.core import safe_unlink
from hk_public_transport_etl.core.errors import InputDataError, TransientError
from tenacity import RetryError, Retrying, retry_if_exception_type, stop_after_attempt
from tenacity.wait import wait_base

_RETRYABLE_STATUSES: set[int] = {408, 429, 500, 502, 503, 504}

log = structlog.get_logger(__name__)


class HttpFetchError(RuntimeError):
    """Base HTTP fetch error."""


class HttpStatusError(HttpFetchError, InputDataError):
    """
    Non-retryable HTTP status (e.g., 400/401/403/404) or any status not in allowed.
    """

    def __init__(
        self,
        *,
        method: str,
        url: str,
        status_code: int,
        body_snippet: str | None,
    ) -> None:
        msg = f"HTTP {status_code} for {method} {url}"
        if body_snippet:
            msg += f" (body: {body_snippet})"
        super().__init__(msg)
        self.method = method
        self.url = url
        self.status_code = status_code


class HttpRetriesExceeded(HttpFetchError, TransientError):
    def __init__(
        self, *, method: str, url: str, attempts: int, last_error: BaseException
    ) -> None:
        super().__init__(
            f"HTTP retries exceeded for {method} {url} (attempts={attempts}): {last_error}"
        )
        self.method = method
        self.url = url
        self.attempts = attempts
        self.last_error = last_error


def make_http_client(
    *,
    timeout: httpx.Timeout | None = None,
    follow_redirects: bool = True,
    user_agent: str = "hk-public-transport-etl/0.1",
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    t = timeout or httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0)
    return httpx.Client(
        timeout=t,
        follow_redirects=follow_redirects,
        headers={"User-Agent": user_agent},
        transport=transport,
    )


def is_retryable_status(code: int) -> bool:
    return code in _RETRYABLE_STATUSES


class DeterministicExponentialBackoff(wait_base):
    def __init__(self, *, base: float = 0.5, cap: float = 4.0) -> None:
        self._base = float(base)
        self._cap = float(cap)

    def __call__(self, retry_state) -> float:
        n = retry_state.attempt_number
        if n <= 1:
            return 0.0
        return min(self._cap, self._base * (2 ** (n - 2)))


@dataclass(frozen=True, slots=True)
class RetryableHttpStatus(Exception):
    method: str
    url: str
    status_code: int


def _retrying(
    *,
    method: str,
    url: str,
    max_attempts: int,
    base: float,
    cap: float,
) -> Retrying:
    def _before_sleep(retry_state) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        sleep = retry_state.next_action.sleep if retry_state.next_action else None
        log.warn(
            "http.retry",
            method=method,
            url=url,
            attempt=retry_state.attempt_number,
            sleep_s=sleep,
            error=repr(exc) if exc else None,
        )

    return Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=DeterministicExponentialBackoff(base=base, cap=cap),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.TransportError, RetryableHttpStatus)
        ),
        reraise=False,
        before_sleep=_before_sleep,
    )


def _run_with_retries(
    *,
    method: str,
    url: str,
    allowed_statuses: set[int],
    max_attempts: int,
    backoff_base: float,
    backoff_cap: float,
    fn,
):
    retrying = _retrying(
        method=method,
        url=url,
        max_attempts=max_attempts,
        base=backoff_base,
        cap=backoff_cap,
    )

    attempt_no = 0

    try:
        for attempt in retrying:
            attempt_no = attempt.retry_state.attempt_number
            with attempt:
                return fn(allowed_statuses)

    except RetryError as re:
        last = re.last_attempt.exception()  # may be None in weird edge cases
        raise HttpRetriesExceeded(
            method=method,
            url=url,
            attempts=re.last_attempt.attempt_number,
            last_error=last or Exception("unknown"),
        ) from last

    except HttpFetchError:
        raise

    except Exception as e:
        raise HttpRetriesExceeded(
            method=method,
            url=url,
            attempts=max(attempt_no, 1),
            last_error=e,
        ) from e

    raise RuntimeError("unreachable")


def _header_value(headers: httpx.Headers, name: str) -> str | None:
    v = headers.get(name)
    v = v.strip() if v else ""
    return v or None


@dataclass(frozen=True, slots=True)
class HttpResponseInfo:
    status_code: int
    final_url: str
    etag: str | None
    last_modified: str | None
    content_type: str | None
    cache_control: str | None
    content_disposition: str | None


def extract_response_info(resp: httpx.Response) -> HttpResponseInfo:
    h = resp.headers
    return HttpResponseInfo(
        status_code=resp.status_code,
        final_url=str(resp.url),
        etag=_header_value(h, "ETag"),
        last_modified=_header_value(h, "Last-Modified"),
        content_type=_header_value(h, "Content-Type"),
        cache_control=_header_value(h, "Cache-Control"),
        content_disposition=_header_value(h, "Content-Disposition"),
    )


def _body_snippet(resp: httpx.Response, *, limit: int = 200) -> str | None:
    """
    Best-effort, bounded snippet for debugging.
    - For non-streaming responses, resp.text is fine.
    - For streaming responses, read a small chunk then stop.
    """
    try:
        # If content already available / non-stream, this is simplest.
        if resp.is_stream_consumed or resp.is_closed:
            s = (resp.text or "")[:limit].strip()
            return s or None
        # Streaming response: consume at most a small chunk.
        buf = bytearray()
        for chunk in resp.iter_bytes(chunk_size=min(4096, limit * 4)):
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) >= limit * 4:
                break
        s = bytes(buf).decode("utf-8", errors="replace")[:limit].strip()
        return s or None
    except Exception:
        return None


def request_with_retries(
    client: httpx.Client,
    *,
    method: str,
    url: str,
    headers: Mapping[str, str] | None = None,
    allowed_statuses: Iterable[int] = (200, 304),
    max_attempts: int = 3,
    backoff_base: float = 0.5,
    backoff_cap: float = 4.0,
) -> httpx.Response:
    allowed = set(allowed_statuses)

    def _do(allowed_statuses: set[int]) -> httpx.Response:
        resp = client.request(method, url, headers=headers)

        if resp.status_code in allowed_statuses:
            return resp

        snippet = _body_snippet(resp)
        resp.close()

        if is_retryable_status(resp.status_code):
            raise RetryableHttpStatus(
                method=method, url=url, status_code=resp.status_code
            )

        raise HttpStatusError(
            method=method,
            url=url,
            status_code=resp.status_code,
            body_snippet=snippet,
        )

    return _run_with_retries(
        method=method,
        url=url,
        allowed_statuses=allowed,
        max_attempts=max_attempts,
        backoff_base=backoff_base,
        backoff_cap=backoff_cap,
        fn=_do,
    )


@dataclass(frozen=True, slots=True)
class HttpDownloadResult:
    info: HttpResponseInfo
    bytes_written: int


def stream_get_to_file_with_retries(
    client: httpx.Client,
    *,
    url: str,
    dest_path: os.PathLike[str] | str,
    headers: Mapping[str, str] | None = None,
    allowed_statuses: Iterable[int] = (200, 304),
    max_attempts: int = 3,
    chunk_bytes: int = 1024 * 128,
    backoff_base: float = 0.5,
    backoff_cap: float = 4.0,
) -> HttpDownloadResult:
    """
    Stream GET into dest_path only when status is 200.
    For 304, no file is written (bytes_written=0).

    Note: Caller should pass a temp path; atomic rename belongs in cache layer.
    """
    dest = Path(dest_path)
    allowed = set(allowed_statuses)

    def _do(allowed_statuses: set[int]) -> HttpDownloadResult:
        safe_unlink(dest)

        with client.stream("GET", url, headers=headers) as resp:
            if resp.status_code not in allowed_statuses:
                snippet = _body_snippet(resp)
                if is_retryable_status(resp.status_code):
                    raise RetryableHttpStatus(
                        method="GET", url=url, status_code=resp.status_code
                    )
                raise HttpStatusError(
                    method="GET",
                    url=url,
                    status_code=resp.status_code,
                    body_snippet=snippet,
                )

            info = extract_response_info(resp)

            if resp.status_code == 304:
                return HttpDownloadResult(info=info, bytes_written=0)

            dest.parent.mkdir(parents=True, exist_ok=True)

            total = 0
            try:
                with dest.open("wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=chunk_bytes):
                        if not chunk:
                            continue
                        f.write(chunk)
                        total += len(chunk)
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                safe_unlink(dest)
                raise

            return HttpDownloadResult(info=info, bytes_written=total)

    try:
        return _run_with_retries(
            method="GET",
            url=url,
            allowed_statuses=allowed,
            max_attempts=max_attempts,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
            fn=_do,
        )
    except Exception:
        safe_unlink(dest)
        raise
