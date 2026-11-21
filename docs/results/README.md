# Measured results (S7 2026-11-20, S8 2026-11-21)

Everything was measured on one PC: Windows 11, Ryzen 7 9800X3D, a home connection in
Indonesia, Docker 29.9.3. The README quotes these files.

| File | What | How |
|---|---|---|
| `backfill-s7.json` | Full USGS backfill, 2015-01-01 to 18:58 UTC on 2026-11-20, into a local folder, with gzip | `uv run python scripts/measure_backfill.py --bronze lake/bronze --out docs/results/backfill-s7.json` (empty `lake/` first) |
| `backfill-s7-minio.json` | The same backfill into MinIO (`compose.yaml`), two minutes later | the same script with `--bronze s3://lake/measure-s7/bronze` and the MinIO variables from `.env.example` |
| `backfill-s7-before-gzip.json` | The first backfill, before `fetch()` asked for gzip | the script as of commit `1a8613f`, which did not yet time storage writes (no `storage_put_seconds_total`) |
| `silver-s8.json` | Silver built from the local bronze: the S7 backfill plus a 30-day USGS look-back and one BMKG snapshot, both fetched at 18:05 UTC on 2026-11-21 (147 objects). Counts, three timed builds and whether they were byte-identical, every BMKG-USGS match with its time and distance, pairs of distinct USGS events within the match tolerance, provinces, and the province step scored against polygons | `uv run quake-lake bmkg` and `uv run quake-lake usgs --days 30` on top of the S7 backfill, then `uv run --with shapely python scripts/measure_silver.py --polygons geoBoundaries-IDN-ADM1_simplified.geojson` (the polygons: geoBoundaries' gbHumanitarian IDN ADM1, simplified, commit `9469f09`, 6.6 MB, not in the repo) |
| `lambda-s7.log` | Three cold starts of the handler in `public.ecr.aws/lambda/python:3.12` under its local emulator, one live invocation each (`{"days": 2}`: three BMKG feeds and two days of USGS), with the emulator's raw `INIT REPORT` / `REPORT` lines and the handler's log | `docker run` with `src/` mounted at `/var/task`, then a `curl` to the invocation URL, as in the CI `docker` job |

## Measured by hand (no file)

- **gzip, interleaved:** the 12 monthly windows of 2018, fetched 3 times in each mode,
  alternating plain and gzip so network drift hits both (36 requests each): median request
  1.027 s plain vs 0.602 s gzip, all 36 in 38.6 s vs 26.7 s, 4.7 MB vs 0.7 MB on the wire.
  This is the fair comparison; the two full backfills above ran 36 minutes apart.
- **MinIO start-up:** `docker compose up -d --wait minio` from a removed container (image
  already pulled, volume kept): 2.4, 2.4 and 2.3 s.
- **Image size:** `quake-lake:dev` was 233,445,845 bytes (`docker image inspect`) in S7,
  mostly boto3 and botocore, and is 688,359,893 bytes with silver's dependencies (S8).
  `du -sm` of its site-packages: pyarrow 170 MB, pandas 81, numpy 43 plus 23 of bundled
  libraries, botocore 31, pandera 7.
- **Silver from the CLI:** `uv run quake-lake silver` on the same bronze as `silver-s8.json`
  prints 1.0 to 1.9 s (imports included), against 0.7 to 0.9 s for `build_silver()` alone.
- **Test suite:** `uv run pytest -q`, three runs: 12.11 to 12.20 s before commit `ef91452`,
  2.14 to 2.15 s after. Review rounds 2 and 3 added 11 tests, one of which waits 0.3 s for a
  stalled server: 2.61 to 2.72 s for the final 106.
