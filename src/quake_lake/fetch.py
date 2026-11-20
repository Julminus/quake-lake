"""HTTP GET with retries, standard library only (the Lambda bundle needs no extra packages)."""

from __future__ import annotations

import time
from collections.abc import Callable
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.request import Request, urlopen

USER_AGENT = "quake-lake/0.1 (+https://github.com/Julminus/quake-lake)"
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
            with urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout) as r:
                body: bytes = r.read()
                return body
        except HTTPError as e:
            detail = " ".join(e.read(300).decode("utf-8", "replace").split())  # one line
            problem = f"HTTP {e.code}: {detail}" if detail else f"HTTP {e.code}"
            if e.code not in RETRY_STATUS:
                raise FetchError(f"GET {url} failed: {problem}") from e
            delay = _seconds(e.headers.get("Retry-After"), backoff)
        except (OSError, HTTPException) as e:  # DNS, refused, reset, timeout, short read
            problem = f"{type(e).__name__}: {getattr(e, 'reason', e)}"
            delay = backoff
        if attempt >= attempts:
            raise FetchError(f"GET {url} failed after {attempt} attempts: {problem}")
        sleep(min(delay, MAX_DELAY))


def _seconds(retry_after: str | None, default: float) -> float:
    """A numeric Retry-After in seconds; anything else (absent, an HTTP-date) is ``default``."""
    try:
        return max(0.0, float(retry_after)) if retry_after else default
    except ValueError:
        return default
