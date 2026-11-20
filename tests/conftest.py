from __future__ import annotations

import io
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError
from urllib.response import addinfourl

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
PALU_EVENTS = 81  # events in usgs-palu-2018-09-28.json


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class FakeNet:
    """Stands in for urllib's urlopen: answers by URL prefix and records each request.

    An answer is a body, an HTTP error status, or a list of those played in turn.
    """

    def __init__(self) -> None:
        self.routes: dict[str, bytes | int | list[bytes | int]] = {
            "https://earthquake.usgs.gov/": fixture("usgs-palu-2018-09-28.json"),
            **{
                f"https://data.bmkg.go.id/DataMKG/TEWS/{feed}.json": fixture(f"bmkg-{feed}.json")
                for feed in ("autogempa", "gempaterkini", "gempadirasakan")
            },
        }
        self.urls: list[str] = []
        self.user_agents: list[str | None] = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.urls.append(url)
        self.user_agents.append(request.get_header("User-agent"))
        answer = next(a for prefix, a in self.routes.items() if url.startswith(prefix))
        if isinstance(answer, list):
            answer = answer.pop(0)
        if isinstance(answer, int):
            raise HTTPError(url, answer, "error", Message(), io.BytesIO(b"server says no"))
        return addinfourl(io.BytesIO(answer), Message(), url, 200)


@pytest.fixture
def net(monkeypatch) -> FakeNet:
    fake = FakeNet()
    monkeypatch.setattr("quake_lake.fetch.urlopen", fake)
    return fake
