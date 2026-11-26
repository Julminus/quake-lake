"""Screenshot the running dashboard with headless Chrome; write docs/img/dashboard.png.

    GOLD_DB=<gold> make dashboard     (in another terminal)
    uv run python scripts/screenshot_dashboard.py --url http://127.0.0.1:8501/

Streamlit draws the page over a websocket after it loads, so Chrome's own --screenshot
catches a loading skeleton. This drives Chrome over the DevTools protocol instead and
waits until the metrics, the map and both charts are on the page.
"""

from __future__ import annotations

import argparse
import base64
import itertools
import json
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

from websockets.sync.client import connect

CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
READY = """
document.querySelectorAll('[data-testid="stMetric"]').length === 4
  && document.querySelector('#deckgl-overlay, canvas') !== null
  && document.querySelectorAll('[data-testid="stVegaLiteChart"] canvas, '
       + '[data-testid="stVegaLiteChart"] svg').length >= 2
"""


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:8501/")
    p.add_argument("--out", default="docs/img/dashboard.png")
    p.add_argument("--chrome", default=CHROME)
    p.add_argument("--width", type=int, default=1400)
    p.add_argument("--height", type=int, default=1300)
    args = p.parse_args()
    port = 9333
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as profile:
        chrome = subprocess.Popen(
            [
                args.chrome,
                "--headless=new",
                f"--remote-debugging-port={port}",
                f"--user-data-dir={profile}",
                "--enable-unsafe-swiftshader",  # WebGL for the map, without a GPU
                "--hide-scrollbars",
                "about:blank",
            ]
        )
        try:
            png = _capture(port, args)
        finally:
            chrome.terminate()
            chrome.wait()
    Path(args.out).write_bytes(png)
    print(f"wrote {args.out} ({len(png):,} bytes)")


def _capture(port: int, args: argparse.Namespace) -> bytes:
    for _ in range(50):  # Chrome takes a moment to open its port
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list") as r:
                page = next(t for t in json.load(r) if t["type"] == "page")
            break
        except OSError:
            time.sleep(0.2)
    else:
        raise SystemExit("Chrome's DevTools port never opened")
    ids = itertools.count(1)
    with connect(page["webSocketDebuggerUrl"], max_size=None) as ws:

        def call(method: str, **params: object) -> dict:
            n = next(ids)
            ws.send(json.dumps({"id": n, "method": method, "params": params}))
            while True:
                msg = json.loads(ws.recv())
                if msg.get("id") == n:
                    if "error" in msg:
                        raise SystemExit(f"{method}: {msg['error']}")
                    return msg["result"]

        call(
            "Emulation.setDeviceMetricsOverride",
            width=args.width,
            height=args.height,
            deviceScaleFactor=1,
            mobile=False,
        )
        light = [{"name": "prefers-color-scheme", "value": "light"}]
        call("Emulation.setEmulatedMedia", features=light)
        call("Page.navigate", url=args.url)
        for _ in range(120):
            if call("Runtime.evaluate", expression=READY)["result"].get("value"):
                break
            time.sleep(0.5)
        else:
            raise SystemExit("the dashboard never finished drawing")
        time.sleep(4)  # the map's basemap tiles come from the network
        return base64.b64decode(call("Page.captureScreenshot", format="png")["data"])


if __name__ == "__main__":
    main()
