"""HTTP GET with retries, standard library only (the Lambda bundle needs no extra packages)."""

from __future__ import annotations

import gzip
import time
import zlib
from collections.abc import Callable
from email.message import Message
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.request import Request, urlopen

USER_AGENT = "quake-lake/0.1 (+https://github.com/Julminus/quake-lake)"
# USGS sends a month of GeoJSON about 7 times smaller gzipped (measured: 4.7 MB -> 0.7 MB
# for the 12 months of 2018). urllib does not decode it by itself; _decoded() does.
HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_DELAY = 60.0

Fetch = Callable[[str], bytes]


class FetchError(RuntimeError):
    """A request that failed for good: a non-retryable status, or retries ran out."""


def fetch(
    url: str,
    *,
    attempts: int = 4,
    timeout: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Return the body of a successful response; retry network errors, 429 and 5xx."""
    attempt = 0
    while True:
        attempt += 1
        backoff = 2.0 ** (attempt - 1)
        try:
            with urlopen(Request(url, headers=HEADERS), timeout=timeout) as r:
                return _decoded(r.read(), r.headers)
        except HTTPError as e:
            try:
                text = _decoded(e.read(), e.headers)[:300]
            except (OSError, EOFError, zlib.error):  # a broken error body: say what we can
                text = b""
            detail = " ".join(text.decode("utf-8", "replace").split())  # one line
            problem = f"HTTP {e.code}: {detail}" if detail else f"HTTP {e.code}"
            if e.code not in RETRY_STATUS:
                raise FetchError(f"GET {url} failed: {problem}") from e
            delay = _seconds(e.headers.get("Retry-After"), backoff)
        # DNS, refused, reset, timeout, short read, a truncated or corrupt gzip body
        except (OSError, HTTPException, EOFError, zlib.error) as e:
            problem = f"{type(e).__name__}: {getattr(e, 'reason', e)}"
            delay = backoff
        if attempt >= attempts:
            raise FetchError(f"GET {url} failed after {attempt} attempts: {problem}")
        sleep(min(delay, MAX_DELAY))


def _decoded(body: bytes, headers: Message) -> bytes:
    """The body as the server meant it: gunzipped if it was sent gzip-encoded."""
    return gzip.decompress(body) if headers.get("Content-Encoding") == "gzip" else body


def _seconds(retry_after: str | None, default: float) -> float:
    """A numeric Retry-After in seconds; anything else (absent, an HTTP-date) is ``default``."""
    try:
        return max(0.0, float(retry_after)) if retry_after else default
    except ValueError:
        return default
