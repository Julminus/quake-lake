# Measured results (S7 2026-11-20, S8 2026-11-21, S9 2026-11-22, S10 2026-11-23, S11 2026-11-25, S12 2026-11-26)

Everything was measured on one PC: Windows 11, Ryzen 7 9800X3D, a home connection in
Indonesia, Docker 29.9.3. The README quotes these files.

| File | What | How |
|---|---|---|
| `pipeline-s12.json` | The quickstart's commands from an empty folder, each a separate process and timed: `quake-lake bmkg`, `quake-lake usgs --since 2015-01-01`, `quake-lake silver`, `dbt build`, `dbt source freshness`; the last lines each printed; bronze's objects and bytes; silver's counts (records, quarantined, repeats, BMKG merges, events); the rows in each mart; the data-quality checks that ran (pandera's typed columns and value checks, counted from the schema; dbt's nodes and their statuses from `run_results.json`); and the dashboard on that gold: its query's rows and time, seven headless renders with Streamlit's `AppTest` (the first in the process, which also warms up imports; three with the rows in Streamlit's cache; three with the cache cleared, so each reads gold again), the size of the map's JSON, the four headline numbers and the top five provinces; `cli_output` writes `<lake>` for the folder. Run at 18:46 UTC on 2026-11-26 on the code after the review's fixes (an earlier run at 18:14 UTC, before them, gave the same counts). `docs/img/dashboard.png` is the same gold in Chrome | `uv run python scripts/measure_pipeline.py --lake <new folder>` (network: about 2.5 minutes); then `GOLD_DB=<lake>/gold.duckdb make dashboard` and `uv run python scripts/screenshot_dashboard.py` |
| `mutations-s12.txt` | 18 bugs planted in `src/quake_lake/dashboard.py` one at a time (a view that ignores the filters, `>=` turned into `>`, NaN in the map, an inner join, a slider for a one-year gold, a cache that never sees a rebuild, ...), with whether `tests/test_dashboard.py` and the gold test that compares the dashboard with the marts failed on each (the unmutated tests pass first): 18 of 18 did. Before the review's fixes to the tests, 8 of a reviewer's 11 survived | `uv run python scripts/mutate_dashboard.py > docs/results/mutations-s12.txt` (about 3 minutes) |
| `backfill-s7.json` | Full USGS backfill, 2015-01-01 to 18:58 UTC on 2026-11-20, into a local folder, with gzip | `uv run python scripts/measure_backfill.py --bronze lake/bronze --out docs/results/backfill-s7.json` (empty `lake/` first) |
| `backfill-s7-minio.json` | The same backfill into MinIO (`compose.yaml`), two minutes later | the same script with `--bronze s3://lake/measure-s7/bronze` and the MinIO variables from `.env.example` |
| `backfill-s7-before-gzip.json` | The first backfill, before `fetch()` asked for gzip | the script as of commit `1a8613f`, which did not yet time storage writes (no `storage_put_seconds_total`) |
| `silver-s8.json` | Silver built from the local bronze: the S7 backfill plus a 30-day USGS look-back and one BMKG snapshot, both fetched at 18:05 UTC on 2026-11-21 (147 objects). Counts, three timed builds and whether they were byte-identical, every BMKG-USGS match with its time and distance, pairs of distinct USGS events within the match tolerance, provinces, and the province step scored against polygons | `uv run quake-lake bmkg` and `uv run quake-lake usgs --days 30` on top of the S7 backfill, then `uv run --with shapely python scripts/measure_silver.py --polygons geoBoundaries-IDN-ADM1_simplified.geojson` (the polygons: geoBoundaries' gbHumanitarian IDN ADM1, simplified, commit `9469f09`, 6.6 MB, not in the repo; https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbHumanitarian/IDN/ADM1/geoBoundaries-IDN-ADM1_simplified.geojson). It also compares the BMKG events in bronze with the recorded fixtures in `tests/fixtures/` |
| `gold-s9.json` | Gold built with dbt from a silver of the same 147 bronze objects as `silver-s8.json`: the dbt nodes and their results, three timed full builds, the mart counts, source freshness, and an incremental run of `fct_earthquakes` after a real fetch (a BMKG snapshot and the 7-day USGS look-back at 18:58 UTC on 2026-11-22), compared with a full refresh | `uv run python scripts/measure_gold.py --bronze lake/bronze` (silver and gold go to a temporary folder; only the fetch writes to the bronze) |
| `airflow-s10.json` | The compose stack from empty volumes: image sizes, `docker compose up -d --wait`, the four runs from 2026-11-21 (`catch_up`) with every task's state, try and duration (Airflow's REST API), memory idle and during a run, a backfill of the 3 days before the start date with each run's USGS command as Airflow rendered it, a manual run over the whole local bronze copied into MinIO, a run with MinIO stopped as it started (then started again once a task was waiting for its retry), and the four marts compared with silver and gold rebuilt outside Docker from a copy of MinIO's bronze, after all nine runs (so `local_rebuild_of_the_same_bronze`'s object, record and event counts are MinIO's bronze at the end, not what the whole-history run read). Measured 20:10 to 20:15 UTC on 2026-11-24, after the review's fixes; the first version (18:45 to 18:50 UTC on 2026-11-23, six runs, no backfill) is in git history at commit `5779c9c` | `uv run python scripts/measure_airflow.py --bronze lake/bronze --yes`: runs `docker compose build`, deletes the stack's volumes, needs the network (the DAG fetches BMKG and USGS live); `--start` defaults to 3 days before today, UTC |
| `airflow-s10-before-gold-fix.json` | The same measurement, run once between the compose and DAG fixes and the gold fix (19:57 to 20:03 UTC on 2026-11-24): the run whose mart comparison found one differing row (see the notes below) | the code of commit `c8b8865` except `agg_province_magnitude.sql`, which was still the version before `ebe0787`; its printed JSON, saved by hand |
| `backfill-overlap-s10.log` | Two 2-day backfills on the compose stack: without `--max-active-runs` both runs started at once and the second's `gold` failed on DuckDB's file lock (then succeeded on its retry); with `--max-active-runs 1` they ran one after the other | by hand, with the commands in the file's header; run times from the REST API, the task lines from each task's attempt-1 log |
| `deadline-s10.log` | The deadline alert firing: the scheduler's two log lines for a manual run whose deadline was set to 1 s after it was queued | by hand: `interval=timedelta(seconds=1)` in `dags/quake_lake_daily.py` (the committed value is 1 hour), a run triggered through the REST API, `docker compose logs airflow-scheduler`; the file's header says which lines |
| `lambda-s11.log` | What the ingest Lambda needs, in `public.ecr.aws/lambda/python:3.12` (pinned by digest) writing to the MinIO from `compose.yaml`, at 18:09 UTC on 2026-11-25: the peak memory (`ru_maxrss`) of the handler's process after a BMKG poll, then a 7-day USGS fetch, then both, in one process (so each value is the peak so far, not the event's own); then, for 128, 256 and 512 MB, three cold starts and one warm invocation each (`{}`: both sources) under the Lambda emulator with `docker --cpus` set to the CPU share Lambda gives that memory (MB / 1,769). Raw emulator lines; their own "Memory Size" and "Max Memory Used" (3008 MB) are the emulator's defaults, not measurements. The network to BMKG and USGS adds noise (warm invocations 0.95 to 3.1 s) | `make minio`, then `bash scripts/measure_lambda.sh > docs/results/lambda-s11.log` |
| `plan-s11.txt` | `terraform plan` of `infra/` with no AWS account (`-var plan_without_credentials=true`): 33 resources (32 before the review added the Lambda's retry setting). The same bytes on Windows and in `hashicorp/terraform:1.17.1` on Linux (compared with `diff`), including the Lambda zip's `source_code_hash` | `make plan`, or the command in the file's first line |
| `mutations-s11.txt` | 48 bugs planted in `infra/` one at a time (each module's permissions, wiring, schedules, settings, formats and lifecycle rules; the root's settings and which bucket each module gets), with whether a `terraform test` run failed on each (the unmutated tests pass first): 47 of 48 did; the one that didn't is the projection's year range | `uv run python scripts/mutate_infra.py > docs/results/mutations-s11.txt` (75 s; `infra/` initialised first) |
| `aws-cost-s11.json` | The monthly AWS cost of `infra/` for months 1 and 12, from the AWS Price List bulk API for `ap-southeast-1` (each price with its source URL and publication date), the usage measured in `lambda-s11.log`, `backfill-s7.json` and `silver-s8.json`, and the assumptions in the file | `uv run python scripts/aws_cost.py` (downloads about 3 MB of price lists); explained in [`../aws-cost-estimate.md`](../aws-cost-estimate.md) |
| `lambda-s7.log` | Three cold starts of the handler in `public.ecr.aws/lambda/python:3.12` under its local emulator, one live invocation each (`{"days": 2}`: three BMKG feeds and two days of USGS), with the emulator's raw `INIT REPORT` / `REPORT` lines and the handler's log | `docker run` with `src/` mounted at `/var/task`, then a `curl` to the invocation URL, as in the CI `docker` job |

## Measured by hand (no file)

- **Test suite, S12:** `uv run pytest -q -p no:cacheprovider`, three runs after the
  review's fixes: 16.44, 16.16 and 16.28 s, 206 passed and 15 skipped on Windows (11 more
  than S11, all in `tests/test_dashboard.py`, 2.8 s together); with `--cov`, 25.2 s (S11's
  suite with `--cov`: 21.6 and 25.1 s, so coverage, not the dashboard, is most of the
  difference). Before the review (8 dashboard tests): 15.04, 15.22 and 15.26 s.
- **The map's payload (S12):** on the gold of the 18:14 UTC run, five `AppTest` reruns each,
  interleaved, before and after the map was sent only what it draws: median rerun 0.186 and
  0.190 s before, 0.155 and 0.148 s after; the map's JSON 7,292,785 bytes before,
  5,586,376 after (default filters).
- **Where `pipeline-s12.json`'s 4 repeats come from:** all from BMKG. Parsing the bronze of
  that run (and of the earlier one) with silver's own readers, its 31 BMKG records carry 27 distinct event ids, and
  the 26,806 USGS events 26,806 distinct ids.
- **A reader blocks `dbt build` (S12):** with one process holding `lake/gold.duckdb` open
  read-only (`duckdb.connect(..., read_only=True)` and a 25 s sleep), `dbt build` in another
  failed with `IO Error: Cannot open file ... The process cannot access the file because it
  is being used by another process`; once the reader closed, the same build passed (52 of
  52). This is why the dashboard reads gold in one go and closes it.
- **Test suite, S11:** `uv run pytest -q -p no:cacheprovider`, three runs (the compose
  stack's MinIO was running, which the tests don't use without `QUAKE_LAKE_MINIO=1`):
  15.61, 13.91 and 13.84 s, 195 passed and 15 skipped on Windows (8 more than S10, all in
  `tests/test_infra.py`: the Glue columns and the cost estimate's parser). Before the
  review rounds added 6 of them: 15.89, 13.88 and 13.95 s.
- **Terraform, S11:** `make infra-check` 6.9 to 8.5 s and `make plan` 3.1 s on this PC
  (three runs each, after the review's fixes) once `infra/` is initialised (`terraform
  plan` alone 2.0 s); the first `terraform init`
  downloaded both providers in 9.8 s. On GitHub the `terraform` job took 30 s (run
  45675288281). The first version of the tests, before the review: of 12 bugs I planted one
  at a time, 11 made `terraform test` fail, but of 14 more a reviewer planted (permissions
  wired to the wrong rule, `s3:*` or `glue:*` in a policy, a lost condition, the pipeline
  policy, partition key types, ...), 13 passed. The tests were rewritten to compare every
  policy whole; then a second reviewer's 24 mutations found the root's wiring untested (the
  Lambda given silver instead of bronze, say) and `mutate_infra.py` counting a Terraform
  that couldn't run as 36 catches. Modules now take whole bucket modules, the wiring is
  asserted through outputs, and the script needs the unmutated tests to pass first;
  `mutations-s11.txt` is the result.

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
- **The local bronze copied into MinIO for `airflow-s10.json`:** 151 objects (145 USGS, 6
  BMKG; `find lake/bronze -type f | wc -l`). The DAG's nine runs added 20 (`mc ls --recursive`
  after the measurement): 4 USGS windows from the four scheduled runs, 3 BMKG feeds from the
  latest of them, 3 USGS windows from the backfill, 3 BMKG + 1 USGS from the whole-history
  run, and from the run with MinIO stopped 1 USGS window and 2 + 3 BMKG feeds: its first
  `bmkg` try snapshotted `gempaterkini` and `gempadirasakan` once MinIO was back but had
  failed on `autogempa`, which goes first, so the task retried and wrote all three again
  (BMKG goes on to the next feed after a failure and fails at the end; silver drops the
  repeats).
- **The mean depth a re-measurement caught:** `airflow-s10-before-gold-fix.json` is the run
  of `measure_airflow.py` after the compose and DAG fixes but before the gold fix (about
  19:57 to 20:03 UTC on 2026-11-24; saved from its printed output, since the final run
  writes to `airflow-s10.json`). Its `local_rebuild_of_the_same_bronze` has 1 row of
  `agg_province_magnitude` in each gold and not the other: Papua Barat Daya's
  minor earthquakes, mean depth 27.4 (Airflow) against 27.3 (local). The 7 depths average
  27.35 exactly; `round(avg(depth_km), 1)` over doubles gave 27.3 on the local fact table and
  27.4 on Airflow's at 1, 4 and 8 DuckDB threads alike, so it was the tables' row order (one
  built in one go, the other over 9 incremental runs), not threading. On the local build it
  disagreed with the exact mean in 2 of 124 groups (5 groups' means end in exactly 5). The
  decimal version agrees with the exact mean in all 124 on both, and the final measurement
  found 0 differing rows. The next review showed that `avg()` of decimals still returns a
  double, so an exact tie such as 100.05 (stored as 100.0499...) could still round down;
  none of the 124 real groups was one. Casting the mean back to `decimal(38, 10)` before
  rounding (the `fix(gold)` commit after `598f768`) rounds it up, which a test checks, and
  gives the same 124 values on the real data as before.
- **Docker's VM:** `docker info` and `docker stats` report 9.7 GiB of memory (9.713 GiB), set
  by `~/.wslconfig` on this PC.
- **Test suite, S10:** `uv run pytest -q`, three runs with the Docker stack stopped: 14.43,
  14.44 and 14.47 s, 187 passed and 15 skipped on Windows (the 12 Airflow DAG tests, the
  Linux-only concurrency test and the two MinIO ones); the 5 dbt tests take 10.1 s of it
  (`--durations`). Before the review's fixes: 12.56 to 12.80 s, 186 passed, 10 skipped. In
  the Airflow image the DAG test file (14 tests) takes about 3 to 4.5 s.
- **The Airflow image's size:** `docker images` reports 2.19 GB for `quake-lake-airflow:dev`
  (2,188,393,915 bytes, `docker image inspect`) and 1.11 GB for
  `apache/airflow:slim-3.3.2-python3.12`. The pipeline's virtualenv is 731 MB (`du -sh
  /app/.venv`, the same as in the CLI image) and its layer 766 MB in `docker history`.
  Before commit `864c365` the image was 3,175,129,061 bytes (3.18 GB; the first measurement
  read 3.17 GB): that layer was 1.49 GB because it also held uv's download cache, 689 MB in
  `/tmp/.cache/uv`, where the Airflow base image sets `UV_CACHE_DIR`. Both sizes are from
  builds on this PC from the same base image, the old one from a fresh clone of `5779c9c`.
- **CI's `airflow` job step running the DAG's tasks (commit `c60d183`):** each `docker
  compose run` with dependencies re-runs `createbucket` and `airflow-init` (`airflow version`
  took 5.6 s that way and 3.2 s with `--no-deps`). With the three tasks in one container,
  the step took 33.7 and 33.9 s against 47.0 and 47.6 s before, interleaved, each from
  fresh volumes in a separate compose project on this PC. On GitHub the whole job went from
  2 min 54 s (run 45455634784) to 2 min 16 s (run 45478037954), one run each.
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
