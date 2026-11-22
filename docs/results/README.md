# Measured results (S7 2026-11-20, S8 2026-11-21, S9 2026-11-22)

Everything was measured on one PC: Windows 11, Ryzen 7 9800X3D, a home connection in
Indonesia, Docker 29.9.3. The README quotes these files.

| File | What | How |
|---|---|---|
| `backfill-s7.json` | Full USGS backfill, 2015-01-01 to 18:58 UTC on 2026-11-20, into a local folder, with gzip | `uv run python scripts/measure_backfill.py --bronze lake/bronze --out docs/results/backfill-s7.json` (empty `lake/` first) |
| `backfill-s7-minio.json` | The same backfill into MinIO (`compose.yaml`), two minutes later | the same script with `--bronze s3://lake/measure-s7/bronze` and the MinIO variables from `.env.example` |
| `backfill-s7-before-gzip.json` | The first backfill, before `fetch()` asked for gzip | the script as of commit `1a8613f`, which did not yet time storage writes (no `storage_put_seconds_total`) |
| `silver-s8.json` | Silver built from the local bronze: the S7 backfill plus a 30-day USGS look-back and one BMKG snapshot, both fetched at 18:05 UTC on 2026-11-21 (147 objects). Counts, three timed builds and whether they were byte-identical, every BMKG-USGS match with its time and distance, pairs of distinct USGS events within the match tolerance, provinces, and the province step scored against polygons | `uv run quake-lake bmkg` and `uv run quake-lake usgs --days 30` on top of the S7 backfill, then `uv run --with shapely python scripts/measure_silver.py --polygons geoBoundaries-IDN-ADM1_simplified.geojson` (the polygons: geoBoundaries' gbHumanitarian IDN ADM1, simplified, commit `9469f09`, 6.6 MB, not in the repo; https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbHumanitarian/IDN/ADM1/geoBoundaries-IDN-ADM1_simplified.geojson). It also compares the BMKG events in bronze with the recorded fixtures in `tests/fixtures/` |
| `gold-s9.json` | Gold built with dbt from a silver of the same 147 bronze objects as `silver-s8.json`: the dbt nodes and their results, three timed full builds, the mart counts, source freshness, and an incremental run of `fct_earthquakes` after a real fetch (a BMKG snapshot and the 7-day USGS look-back at 18:58 UTC on 2026-11-22), compared with a full refresh | `uv run python scripts/measure_gold.py --bronze lake/bronze` (silver and gold go to a temporary folder; only the fetch writes to the bronze) |
| `lambda-s7.log` | Three cold starts of the handler in `public.ecr.aws/lambda/python:3.12` under its local emulator, one live invocation each (`{"days": 2}`: three BMKG feeds and two days of USGS), with the emulator's raw `INIT REPORT` / `REPORT` lines and the handler's log | `docker run` with `src/` mounted at `/var/task`, then a `curl` to the invocation URL, as in the CI `docker` job |

## Measured by hand (no file)

- **gzip, interleaved:** the 12 monthly windows of 2018, fetched 3 times in each mode,
  alternating plain and gzip so network drift hits both (36 requests each): median request
  1.027 s plain vs 0.602 s gzip, all 36 in 38.6 s vs 26.7 s, 4.7 MB vs 0.7 MB on the wire.
  This is the fair comparison; the two full backfills above ran 36 minutes apart.
- **MinIO start-up:** `docker compose up -d --wait minio` from a removed container (image
  already pulled, volume kept): 2.4, 2.4 and 2.3 s.
- **Image size:** `quake-lake:dev` was 233,445,845 bytes (`docker image inspect`) in S7,
  mostly boto3 and botocore, 688,359,893 bytes with silver's dependencies (S8), and is
  1,182,854,459 bytes with dbt-duckdb (S9). `du -sm` of its site-packages: pyarrow 170 MB,
  pandas 81, DuckDB 66, numpy 43 plus 23 of bundled libraries, Babel 33 (from dbt's agate),
  botocore 31, networkx 19, rapidfuzz 12, dbt 11, sqlglot 9, pandera 7.
- **Gold from the CLI:** `make gold` (`uv run dbt build`, with dbt's start-up and a parse
  that can reuse the last one) took 2.69, 2.70 and 2.70 s on the S8 silver; importing
  `dbt.cli.main` alone takes about 1 s. In a fresh clone the first `make gold`, which parses
  the whole project, took 5.0 s (on the quickstart's 2026 silver; repeat runs 2.5 s).
- **Gaps between events (for the freshness threshold):** over the S8 silver, ordered by
  `time`, the longest gap between two consecutive events is 49.7 hours (2 days 01:41:54);
  148 gaps exceed 24 hours, 15 exceed 36 and 1 exceeds 48. DuckDB: `with t as (select time
  - lag(time) over (order by time) as gap from read_parquet('lake/silver/events/*/*/*.parquet'))
  select max(gap) from t`.
- **The busiest day, by the town USGS names:** of the 135 Indonesian earthquakes on
  2026-04-02 (UTC) in the S8 silver's gold, 71 are "... of Bitung", 61 "... of Ternate"
  (all 132 between 0.80 and 1.53°N), and one each near Tobelo (3.27°N, off the Talaud
  Islands), Nabire and Pelabuhanratu. The 132 split 69 to Sulawesi Utara and 63 to Maluku
  Utara by the nearest-province rule. The M7.4 before them is in `gold-s9.json`
  (`largest_around_the_busiest_day`). Query: `select regexp_extract(place, 'of (.*),', 1),
  province_code, count(*), min(latitude), max(latitude) from fct_earthquakes where
  in_indonesia and origin_date_utc = '2026-04-02' group by all`.
- **Test suite, S9:** `uv run pytest -q`, three runs: 12.7 to 14.1 s, 184 passed and 3
  skipped on Windows (the Linux-only concurrency test and the two MinIO ones); after the
  review rounds added dbt runs to them, 12.7 to 12.8 s, of which the 4 dbt tests take about
  7.3 s (`--durations`), and collecting them (importing dbt) much of the rest. `make test`
  20.4 s, then 20.8 s.
- **Silver from the CLI:** `uv run quake-lake silver` on the same bronze as `silver-s8.json`
  prints 1.0 to 1.2 s (the pandas, pyarrow and pandera imports included), against 0.5 to
  0.7 s for `build_silver()` alone. Importing `quake_lake.cli` takes 205 to 274 ms and
  `quake_lake.silver` 595 to 658 ms (10 runs each), which is why the CLI imports silver only
  for the `silver` command.
- **Silver I/O and Parquet writing (commit `277baee`):** median of 5 rebuilds, interleaved
  with the commit before it, byte-identical output: a local folder (the 147 objects above)
  0.78 s to 0.57 s, and MinIO (`s3://lake/measure-s7/bronze`, the 143-object backfill) 2.17 s
  to 1.03 s. Under a profiler before it, the MinIO build spent 1.3 s in puts and 0.5 s in
  gets of 2.9 s.
- **Test suite, S8:** `uv run pytest -q`, three runs: 4.5 to 4.9 s for 181 offline tests
  (183 with the two MinIO tests); `make test`, which adds coverage, 7.1 s (two runs).
- **Test suite, S7:** `uv run pytest -q`, three runs: 12.11 to 12.20 s before commit `ef91452`,
  2.14 to 2.15 s after. Review rounds 2 and 3 added 11 tests, one of which waits 0.3 s for a
  stalled server: 2.61 to 2.72 s for the final 106.
