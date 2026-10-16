#!/usr/bin/env bash
# Pushes TOTAL spans through the pipeline as fast as it'll go, then reports
# send time, consumer drain time, and stats-query latency at that volume.
#
# Usage: ./scripts/load_test.sh [total_spans]
set -euo pipefail

TOTAL="${1:-10000000}"
CONCURRENCY="${CONCURRENCY:-24}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
API_URL="${API_URL:-http://localhost:8082}"

echo "== load test: $TOTAL spans, concurrency=$CONCURRENCY, batch-size=$BATCH_SIZE =="

start=$(date +%s)
docker compose --profile tools run --rm generator \
  -total "$TOTAL" -rate 100000000 -concurrency "$CONCURRENCY" -batch-size "$BATCH_SIZE"
sent_done=$(date +%s)

echo "== waiting for consumer to drain =="
while true; do
  lag=$(docker compose exec redpanda rpk group describe spans-consumer 2>/dev/null | awk '/TOTAL-LAG/{print $2}')
  echo "lag=$lag"
  [ "$lag" = "0" ] && break
  sleep 2
done
drained=$(date +%s)

echo "== stats query latency (20 samples each) =="
for path in tool-failures run-costs slow-steps loops; do
  times=()
  for _ in $(seq 1 20); do
    t=$(curl -s -o /dev/null -w '%{time_total}' "$API_URL/v1/stats/$path")
    times+=("$t")
  done
  sorted=($(printf '%s\n' "${times[@]}" | sort -n))
  count=${#sorted[@]}
  mid=$((count / 2))
  p95_idx=$(((count * 95) / 100))
  if [ "$p95_idx" -ge "$count" ]; then p95_idx=$((count - 1)); fi
  printf "%-15s p50=%ss p95=%ss\n" "$path" "${sorted[$mid]}" "${sorted[$p95_idx]}"
done

echo "== summary =="
echo "send time:  $((sent_done - start))s"
echo "drain time: $((drained - sent_done))s"
echo "total time: $((drained - start))s"
