# Benchmark results


## Reproduce

```sh
docker compose down -v   
docker compose up -d
make load-test           
```

`load_test.sh` sends the given number of spans at an uncapped rate, waits for the consumer group's lag to hit zero, then times each stats endpoint 20 times and reports p50/p95.

## Ingest throughput

One clean run, `./scripts/load_test.sh 10000000` (concurrency=24, batch-size=5000) against an empty table:

| Stage | Spans | Time | Rate |
|---|---|---|---|
| Generator -> ingest -> Redpanda (send) | 10,115,116 | 35.3s | 286,243 spans/sec |
| Consumer -> ClickHouse (drain remaining backlog) | 6,855,116 | 77s | 89,027 spans/sec |
| End-to-end (send start to fully drained) | 10,115,116 | ~112s | ~90,313 spans/sec |

The consumer/ClickHouse insert is the actual bottleneck, not ingest or Redpanda - ingest can accept spans over 3x faster than the pipeline can durably land them. This matches the design: ingest is deliberately decoupled from storage so a slow consumer never blocks agents sending traces (see README design decisions).

No data loss: every span sent was confirmed present in ClickHouse afterward (`SELECT count()`).

## Query latency at ~10.6M rows

20 sequential requests per endpoint (single client, no concurrency), against a table with 10,610,293 rows / 976,965 distinct runs.

| Endpoint | p50 | p95 |
|---|---|---|
| `/v1/stats/tool-failures` | 38ms | 66-112ms |
| `/v1/stats/run-costs` | 124-126ms | 154-187ms |
| `/v1/stats/slow-steps` | 85-88ms | 105-126ms |
| `/v1/stats/loops` | 1.35-1.37s | 2.15-2.74s |

`loops` is the clear outlier: it groups by `(run_id, tool_name, input_hash)`, which doesn't align with the table's `ORDER BY (run_id, tool_name, start_time)` the way the other three queries' groupings do, so it scans and aggregates over the full table rather than a narrower slice. See the README's "Hardest problem" section for how this was diagnosed and what it would take to fix.
