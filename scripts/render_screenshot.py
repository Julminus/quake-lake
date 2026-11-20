"""Render docs/img/ingest.svg from a real run of the CLI (needs the network).

Runs ``quake-lake usgs`` and ``quake-lake bmkg`` against the given bronze root
and records exactly what they print. Rich is only needed here, so pull it in
for this one run:

    uv run --with rich python scripts/render_screenshot.py s3://lake/bronze
"""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

from rich.console import Console

OUT = Path(__file__).resolve().parents[1] / "docs" / "img" / "ingest.svg"


def main(bronze: str) -> None:
    console = Console(record=True, width=104, force_terminal=True, file=io.StringIO())
    for args in (["usgs", "--days", "7"], ["bmkg"]):
        cmd = ["quake-lake", *args, "--bronze", bronze]
        run = subprocess.run(cmd, capture_output=True, text=True, check=True)
        console.print(f"$ {' '.join(cmd)}", style="bold", markup=False, highlight=False)
        console.print(run.stdout.rstrip(), markup=False, highlight=False)
    svg = console.export_svg(title="quake-lake")
    # LF endings and no trailing spaces, whichever OS renders it.
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(line.rstrip() for line in svg.splitlines()) + "\n", newline="\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "lake/bronze")
