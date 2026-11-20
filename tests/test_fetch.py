from __future__ import annotations

import gzip
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from quake_lake.fetch import USER_AGENT, FetchError, _seconds, fetch


class Server:
    """A real local HTTP server that plays back a script of (status, headers, body).

    Status 0 hangs up without answering, which the client sees as a network error.
    A header "X-Stall: <seconds>" keeps the connection open that long after the body.
    """

    def __init__(self) -> None:
        self.script: list[tuple[int, dict[str, str], bytes]] = []
        self.user_agents: list[str | None] = []
        self.accept_encodings: list[str | None] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                server.user_agents.append(self.headers.get("User-Agent"))
                server.accept_encodings.append(self.headers.get("Accept-Encoding"))
                status, headers, body = server.script.pop(0)
                if status == 0:
                    self.close_connection = True
                    return
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                if "Content-Length" not in headers:  # a script may lie about it
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
                time.sleep(float(headers.get("X-Stall", 0)))

            def log_message(self, *args: object) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/q"


@pytest.fixture
def server() -> Iterator[Server]:
    s = Server()
    # shutdown() waits for the next poll: the default 0.5 s cost every test half a second.
    thread = threading.Thread(target=s.httpd.serve_forever, args=(0.01,), daemon=True)
    thread.start()
    yield s
    s.httpd.shutdown()
    s.httpd.server_close()


def test_returns_body_and_sends_user_agent(server):
    server.script = [(200, {}, b'{"ok": 1}')]
    assert fetch(server.url) == b'{"ok": 1}'
    assert server.user_agents == [USER_AGENT]


def test_asks_for_gzip_and_returns_the_decoded_body(server):
    body = b'{"type": "FeatureCollection", "features": []}' * 50
    server.script = [(200, {"Content-Encoding": "gzip"}, gzip.compress(body))]
    assert fetch(server.url) == body
    assert server.accept_encodings == ["gzip"]


def test_a_server_that_ignores_accept_encoding_still_works(server):
    server.script = [(200, {}, b"plain")]
    assert fetch(server.url) == b"plain"


def test_a_gzip_error_body_is_decoded_into_the_message(server):
    server.script = [
        (400, {"Content-Encoding": "gzip"}, gzip.compress(b"Error 400: bad starttime"))
    ]
    with pytest.raises(FetchError, match=r"HTTP 400: Error 400: bad starttime$"):
        fetch(server.url, sleep=lambda s: None)


def _corrupt_deflate() -> bytes:
    body = bytearray(gzip.compress(b"x" * 1000))
    body[10] ^= 0xFF  # the first byte after the gzip header: zlib.error, "invalid code lengths"
    return bytes(body)


# One body for each way gzip.decompress fails: OSError (BadGzipFile), EOFError, zlib.error.
BAD_GZIP = [b"not gzip at all", gzip.compress(b"x" * 1000)[:20], _corrupt_deflate()]


@pytest.mark.parametrize("encoding", ["GZIP", "x-gzip", " gzip "])
def test_content_encoding_is_read_case_insensitively(server, encoding):
    server.script = [(200, {"Content-Encoding": encoding}, gzip.compress(b"{}"))]
    assert fetch(server.url) == b"{}"


@pytest.mark.parametrize("bad", BAD_GZIP)
def test_a_corrupt_or_cut_gzip_body_is_retried_then_reported(server, bad):
    server.script = [(200, {"Content-Encoding": "gzip"}, bad)] * 2
    slept: list[float] = []
    with pytest.raises(FetchError, match="after 2 attempts"):
        fetch(server.url, attempts=2, sleep=slept.append)
    assert slept == [1.0]


@pytest.mark.parametrize("bad", BAD_GZIP)
def test_a_corrupt_gzip_error_body_still_gives_the_status(server, bad):
    server.script = [(404, {"Content-Encoding": "gzip"}, bad)]
    with pytest.raises(FetchError, match=r"failed: HTTP 404$"):
        fetch(server.url)


def test_a_stalled_error_body_is_retried_not_raised(server):
    # A 503 promising 100 bytes, sending 5, then going quiet: reading its reason times out.
    stall = {"Content-Length": "100", "X-Stall": "0.6"}
    server.script = [(503, stall, b"short"), (200, {}, b"ok")]
    slept: list[float] = []
    assert fetch(server.url, timeout=0.3, sleep=slept.append) == b"ok"
    assert slept == [1.0]


def test_a_cut_error_body_still_gives_its_reason(server):
    server.script = [(400, {"Content-Length": "2000"}, b"Error 400: bad starttime")]
    with pytest.raises(FetchError, match=r"failed: HTTP 400: Error 400: bad starttime$"):
        fetch(server.url)


def test_retries_5xx_with_backoff(server):
    server.script = [(503, {}, b""), (502, {}, b""), (200, {}, b"done")]
    slept: list[float] = []
    assert fetch(server.url, sleep=slept.append) == b"done"
    assert slept == [1.0, 2.0]


def test_honours_retry_after_and_caps_it(server):
    server.script = [
        (429, {"Retry-After": "7"}, b""),
        (429, {"Retry-After": "86400"}, b""),
        (200, {}, b"ok"),
    ]
    slept: list[float] = []
    assert fetch(server.url, sleep=slept.append) == b"ok"
    assert slept == [7.0, 60.0]


def test_client_error_fails_at_once_with_the_servers_reason(server):
    server.script = [
        (400, {}, b"Error 400: Bad Request\n\n26761 matching events exceeds search limit of 20000.")
    ]
    slept: list[float] = []
    with pytest.raises(
        FetchError, match=r"HTTP 400: Error 400: Bad Request.*exceeds search limit"
    ) as e:
        fetch(server.url, sleep=slept.append)
    assert slept == []
    assert server.url in str(e.value)


def test_gives_up_after_the_last_attempt(server):
    server.script = [(500, {}, b"")] * 3
    slept: list[float] = []
    with pytest.raises(FetchError, match="after 3 attempts: HTTP 500"):
        fetch(server.url, attempts=3, sleep=slept.append)
    assert slept == [1.0, 2.0]


def test_network_errors_are_retried_then_reported(server):
    server.script = [(0, {}, b""), (0, {}, b"")]  # the server hangs up, twice
    slept: list[float] = []
    with pytest.raises(FetchError, match="after 2 attempts: RemoteDisconnected"):
        fetch(server.url, attempts=2, sleep=slept.append)
    assert slept == [1.0]


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, 4.0),
        ("", 4.0),
        ("3", 3.0),
        ("1.5", 1.5),
        ("-2", 0.0),
        ("Wed, 21 Oct 2026 07:28:00 GMT", 4.0),
    ],
)
def test_retry_after_parsing(header, expected):
    assert _seconds(header, 4.0) == expected
