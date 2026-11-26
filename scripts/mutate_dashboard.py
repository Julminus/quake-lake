"""Plant bugs in the dashboard one at a time and check that a test fails on each.

    uv run python scripts/mutate_dashboard.py --out docs/results/mutations-s12.txt

Runs tests/test_dashboard.py and the gold test that compares the dashboard with the marts
(6 to 10 s for a clean run, less when a test fails) on the unmutated code, which must
pass, then on each bug, then on the unmutated code again. Only pytest's "some tests
failed" counts as a catch; any other exit (a collection or usage error) stops the run.
dashboard.py must match the last commit, and is restored at the end; the report is
written only when every run is done.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
SRC = ROOT / "src" / "quake_lake" / "dashboard.py"
TESTS = ["tests/test_dashboard.py", "tests/test_gold.py::test_gold_builds_from_the_recorded_feeds"]
MAP_START = "        # Only what the map draws"
MAP_END = '    with right:\n        st.subheader("By province")'
# name: (code, its replacement), ...; each code must occur once.
MUTATIONS: dict[str, list[tuple[str, str]]] = {
    "the map ignores the filters": [
        (MAP_START, "        shown, kept = quakes, shown\n" + MAP_START),
        (MAP_END, "        shown = kept\n" + MAP_END),
    ],
    "the month chart ignores the filters": [("monthly(shown)", "monthly(quakes)")],
    "the magnitude chart ignores the filters": [("magnitudes(shown)", "magnitudes(quakes)")],
    "minimum magnitude: >= becomes >": [("magnitude >= min_mag", "magnitude > min_mag")],
    "the abroad toggle is ignored": [("if not abroad:", "if False:")],
    "the magnitude filter drops unknowns at its lowest": [("if min_mag > lowest:", "if True:")],
    "M5 and up: >= becomes >": [("(shown.magnitude >= 5)", "(shown.magnitude > 5)")],
    "M6 and up: >= becomes >": [("(shown.magnitude >= 6)", "(shown.magnitude > 6)")],
    "the table's M5+: >= becomes >": [("quakes.magnitude >= 5", "quakes.magnitude > 5")],
    "the table's largest is the smallest": [('("magnitude", "max")', '("magnitude", "min")')],
    "no depth colours (NaN in the map)": [("map(DEPTH_RGB)", "map({})")],
    "no radius without a magnitude (NaN)": [("magnitude.fillna(3) - 3", "magnitude - 3")],
    "the radius shrinks with magnitude": [
        ("2 ** (shown.magnitude.fillna(3) - 3)", "2 ** (3 - shown.magnitude.fillna(3))")
    ],
    "longitude from latitude": [('"lon": shown.longitude', '"lon": shown.latitude')],
    "the layer reads latitude as x": [
        ('get_position=["lon", "lat"]', 'get_position=["lat", "lon"]')
    ],
    "the tooltip shows only the place": [
        ('tooltip={"text": "{tip}"}', 'tooltip={"text": "{place}"}')
    ],
    "the tooltip loses the place": [('"\\n" + shown.place + "\\n"', '"\\n"')],
    "the tooltip's time is minutes:hours": [('"%Y-%m-%d %H:%M UTC"', '"%Y-%m-%d %M:%H UTC"')],
    "depth to the whole km": [("depth_km.round(1)", "depth_km.round(0)")],
    'depth "-0.0"': [("(shown.depth_km.round(1) + 0.0)", "shown.depth_km.round(1)")],
    "largest drawn first": [("nulls first  --", "desc nulls last  --")],
    "the magnitude slider stops at 8.0": [("MAX_MAGNITUDE = 10.0", "MAX_MAGNITUDE = 8.0")],
    "the magnitude slider starts at 0": [
        ('"Minimum magnitude", lowest, MAX', '"Minimum magnitude", 0.0, MAX')
    ],
    "the largest's help names its id": [("UTC, {top.place}", "UTC, {top.event_id}")],
    "the caption's years swapped": [("{years[0]} to {years[1]}", "{years[1]} to {years[0]}")],
    "newest event from the filtered rows": [
        ("quakes.origin_time_utc.max()", "shown.origin_time_utc.max()")
    ],
    "an inner join to dim_province": [("left join dim_province", "join dim_province")],
    "no province: dropped from the table": [
        (", f.province_code, 'province unknown')", ", f.province_code)")
    ],
    "no place: missing from the tooltip": [
        ("coalesce(f.place, 'place unknown') as place", "f.place")
    ],
    "a year slider for a one-year gold": [("if first < last:", "if True:")],
    "the largest of no magnitudes": [("if shown.magnitude.notna().any():", "if True:")],
    "a cache that never sees a rebuild": [("Path(path).stat().st_mtime_ns", "0")],
    "a cache that keeps every version": [(", max_entries=1)", ")")],
}


def tests_fail() -> bool:
    cmd = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *TESTS]
    code = subprocess.run(cmd, cwd=ROOT, capture_output=True).returncode
    if code not in (0, 1):  # 1: tests failed; anything else is not a catch
        raise SystemExit(f"pytest exited {code}: not a test failure")
    return code == 1


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=ROOT / "docs" / "results" / "mutations-s12.txt")
    args = p.parse_args()
    dirty = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", str(SRC)], cwd=ROOT).returncode
    if dirty:
        raise SystemExit(f"{SRC} has uncommitted changes; commit or stash them first")
    if not args.out.parent.is_dir():  # the report is written last; fail before the runs
        raise SystemExit(f"no folder {args.out.parent}")
    original = SRC.read_text(encoding="utf-8")
    lines = []
    try:
        if tests_fail():
            raise SystemExit("the tests fail on the unmutated code")
        for name, edits in MUTATIONS.items():
            mutated = original
            for code, bug in edits:
                if original.count(code) != 1:
                    raise SystemExit(f"{name}: {code!r} occurs {original.count(code)} times")
                mutated = mutated.replace(code, bug)
            SRC.write_text(mutated, encoding="utf-8", newline="\n")
            lines.append(f"{'caught' if tests_fail() else 'MISSED'}  {name}")
            print(lines[-1], file=sys.stderr, flush=True)
    finally:
        SRC.write_text(original, encoding="utf-8", newline="\n")
    if tests_fail():
        raise SystemExit("the tests fail on the restored code")
    caught = sum(line.startswith("caught") for line in lines)
    lines.append(f"{caught} of {len(MUTATIONS)} caught")
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(lines[-1])


if __name__ == "__main__":
    main()
