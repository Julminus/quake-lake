#!/usr/bin/env bash
# What the ingest Lambda needs, measured in AWS's Lambda image, writing to the MinIO from
# compose.yaml (`make minio` first). Prints raw lines; docs/results/lambda-s11.log is one run.
#
#   1. peak memory (ru_maxrss) of the handler's Python process, per event;
#   2. cold start + one warm invocation at the CPU share Lambda gives each memory size
#      (1,769 MB = 1 vCPU, so `docker --cpus` = MB / 1769), from the emulator's REPORT lines.
#      The emulator's own "Memory Size" and "Max Memory Used" are not the limit; ignore them.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
img=public.ecr.aws/lambda/python:3.12@sha256:f229f304f82f5016fe26e4714d05b6a8196e0c6c61b463112841a5e3c3647163
s3=(--network quake-lake_default -v "$PWD/src:/var/task:ro" -e BRONZE_URI=s3://lake/bronze-measure-lambda
    -e AWS_ENDPOINT_URL_S3=http://minio:9000 -e AWS_ACCESS_KEY_ID=quakelake
    -e AWS_SECRET_ACCESS_KEY=quakelake-local -e AWS_REGION=us-east-1)
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: leave /var/task alone

echo "# $(date -u +%FT%TZ) peak memory of the handler process (BMKG, then USGS 7 days, then both)"
docker run --rm "${s3[@]}" -w /var/task --entrypoint python "$img" -c "
import json, resource, time
from quake_lake.handler import lambda_handler
for ev in ({'sources': ['bmkg']}, {'sources': ['usgs'], 'days': 7}, {}):
    t = time.perf_counter()
    r = lambda_handler(ev)
    print(json.dumps({'event': ev, 'seconds': round(time.perf_counter() - t, 3),
        'objects': len(r['objects']), 'bytes': sum(o['size'] for o in r['objects']),
        'maxrss_mib': round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)}))
" 2>&1 | grep '^{'

echo "# $(date -u +%FT%TZ) cold start by memory size; event {} (3 BMKG feeds + 7 days of USGS)"
for mb in 128 256 512; do
  cpus=$(python -c "print(round($mb / 1769, 4))")
  for rep in 1 2 3; do
    docker run -d --rm --name measure-lambda --cpus "$cpus" -p 127.0.0.1:9100:8080 "${s3[@]}" \
      "$img" quake_lake.handler.lambda_handler > /dev/null
    for _ in $(seq 30); do  # the first request that gets through is the cold invocation
      curl -sf -d '{}' http://127.0.0.1:9100/2015-03-31/functions/function/invocations > /dev/null && break
      sleep 1
    done
    curl -sf -d '{}' http://127.0.0.1:9100/2015-03-31/functions/function/invocations > /dev/null
    echo "## memory ${mb} MB, --cpus ${cpus}, repeat ${rep}: cold invocation, then a warm one"
    docker logs measure-lambda 2>&1 | grep -E "INIT REPORT|^REPORT"
    docker stop measure-lambda > /dev/null
  done
done
# The objects written above are throwaway; the pipeline's bronze (s3://lake/bronze) is untouched.
docker compose run --rm --entrypoint mc createbucket rm --recursive --force local/lake/bronze-measure-lambda > /dev/null 2>&1
