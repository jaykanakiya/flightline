CREATE TABLE IF NOT EXISTS spans
(
    trace_id        String,
    span_id         String,
    parent_span_id  String,
    agent_name      LowCardinality(String),
    run_id          String,
    step_type       LowCardinality(String),
    tool_name       LowCardinality(String),
    model           LowCardinality(String),
    input_tokens    UInt32,
    output_tokens   UInt32,
    cost_usd        Float64,
    latency_ms      UInt32,
    status          LowCardinality(String),
    error_message   String,
    input_hash      String,
    start_time      DateTime64(3),
    end_time        DateTime64(3),
    attributes      Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toDate(start_time)
ORDER BY (run_id, tool_name, start_time)
TTL toDate(start_time) + INTERVAL 30 DAY;

-- tool-failures and slow-steps group by (step_type, tool_name) without
-- run_id, so they don't benefit from the primary key order above.
ALTER TABLE spans ADD INDEX IF NOT EXISTS idx_step_type_tool (step_type, tool_name) TYPE set(0) GRANULARITY 4;
