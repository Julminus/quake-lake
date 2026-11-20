from __future__ import annotations

import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from quake_lake.fetch import USER_AGENT, FetchError, _seconds, fetch


class Server:
    """A real local HTTP server that plays back a script of (status, headers, body)."""

    def __init__(self) -> None:
        self.script: list[tuple[int, dict[str, str], bytes]] = []
        self.user_agents: list[str | None] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                server.user_agents.append(self.headers.get("User-Agent"))
                status, headers, body = server.script.pop(0)
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/q"


@pytest.fixture
def server() -> Iterator[Server]:
    s = Server()
    thread = threading.Thread(target=s.httpd.serve_forever, daemon=True)
    thread.start()
    yield s
    s.httpd.shutdown()
    s.httpd.server_close()


def test_returns_body_and_sends_user_agent(server):
    server.script = [(200, {}, b'{"ok": 1}')]
    assert fetch(server.url) == b'{"ok": 1}'
    assert server.user_agents == [USER_AGENT]


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


def test_network_errors_are_retried_then_reported():
    with socket.socket() as s:  # a port nothing listens on
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    slept: list[float] = []
    with pytest.raises(FetchError, match="after 2 attempts: URLError"):
        fetch(f"http://127.0.0.1:{port}/", attempts=2, timeout=5, sleep=slept.append)
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
