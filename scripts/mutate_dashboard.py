"""Plant bugs in the dashboard one at a time and check that a test fails on each.

    uv run python scripts/mutate_dashboard.py > docs/results/mutations-s12.txt

Runs tests/test_dashboard.py and the gold test that compares the dashboard with the marts
(about 10 s a run), first on the unmutated code, which must pass. The file is restored
at the end, whatever happens.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).parent.parent / "src" / "quake_lake" / "dashboard.py"
TESTS = ["tests/test_dashboard.py", "tests/test_gold.py::test_gold_builds_from_the_recorded_feeds"]
MUTATIONS = {  # name: (code, its replacement); each code must occur once
    "the map ignores the filters": (
        "        # Only what the map draws",
        "        shown, kept = quakes, shown\n        # Only what the map draws",
    ),
    "the month chart ignores the filters": ("monthly(shown)", "monthly(quakes)"),
    "the magnitude chart ignores the filters": ("magnitudes(shown)", "magnitudes(quakes)"),
    "minimum magnitude: >= becomes >": ("shown.magnitude >= min_mag", "shown.magnitude > min_mag"),
    "the abroad toggle is ignored": ("if not abroad:", "if False:"),
    "the magnitude filter drops unknowns at its lowest": ("if min_mag > lowest:", "if True:"),
    "M5 and up: >= becomes >": ("(shown.magnitude >= 5)", "(shown.magnitude > 5)"),
    "M6 and up: >= becomes >": ("(shown.magnitude >= 6)", "(shown.magnitude > 6)"),
    "the table's M5+: >= becomes >": (
        "m5_plus=quakes.magnitude >= 5",
        "m5_plus=quakes.magnitude > 5",
    ),
    "the table's largest is the smallest": ('("magnitude", "max")', '("magnitude", "min")'),
    "no depth colours (NaN in the map)": ("depth_class.map(DEPTH_RGB)", "depth_class.map({})"),
    "no radius without a magnitude (NaN)": ("magnitude.fillna(3) - 3", "magnitude - 3"),
    "the tooltip loses the place": ('"\\n" + shown.place.fillna("") + "\\n"', '"\\n"'),
    "newest event from the filtered rows": (
        "quakes.origin_time_utc.max()",
        "shown.origin_time_utc.max()",
    ),
    "an inner join to dim_province": ("left join dim_province", "join dim_province"),
    "a year slider for a one-year gold": ("if first < last:", "if True:"),
    "the largest of no magnitudes": ("if shown.magnitude.notna().any():", "if True:"),
    "a cache that never sees a rebuild": ("Path(path).stat().st_mtime_ns", "0"),
}


def failed() -> bool:
    cmd = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *TESTS]
    return subprocess.run(cmd, cwd=SRC.parents[2], capture_output=True).returncode != 0


def main() -> None:
    original = SRC.read_text(encoding="utf-8")
    try:
        if failed():
            raise SystemExit("the tests fail on the unmutated code")
        caught = 0
        for name, (code, bug) in MUTATIONS.items():
            if original.count(code) != 1:
                raise SystemExit(f"{name}: {code!r} occurs {original.count(code)} times")
            SRC.write_text(original.replace(code, bug), encoding="utf-8", newline="\n")
            hit = failed()
            caught += hit
            print(f"{'caught  ' if hit else 'MISSED  '} {name}", flush=True)
        print(f"{caught} of {len(MUTATIONS)} caught")
    finally:
        SRC.write_text(original, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
