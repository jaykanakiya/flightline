package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Queries struct {
	conn clickhouse.Conn
}

func NewQueries(conn clickhouse.Conn) *Queries {
	return &Queries{conn: conn}
}

type ToolFailure struct {
	ToolName    string  `json:"tool_name"`
	Failures    uint64  `json:"failures"`
	Total       uint64  `json:"total"`
	FailureRate float64 `json:"failure_rate"`
}

// ToolFailures returns the failure rate per tool over the last sinceHours.
func (q *Queries) ToolFailures(ctx context.Context, sinceHours int) ([]ToolFailure, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT tool_name,
		       countIf(status != 'ok') AS failures,
		       count() AS total,
		       failures / total AS failure_rate
		FROM spans
		WHERE step_type = 'tool_call'
		  AND tool_name != ''
		  AND start_time >= now() - INTERVAL ? HOUR
		GROUP BY tool_name
		ORDER BY failure_rate DESC
	`, sinceHours)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ToolFailure
	for rows.Next() {
		var r ToolFailure
		if err := rows.Scan(&r.ToolName, &r.Failures, &r.Total, &r.FailureRate); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

type RunCost struct {
	RunID             string  `json:"run_id"`
	TotalCostUSD      float64 `json:"total_cost_usd"`
	TotalInputTokens  uint64  `json:"total_input_tokens"`
	TotalOutputTokens uint64  `json:"total_output_tokens"`
}

// RunCosts returns the top `limit` runs by total cost.
func (q *Queries) RunCosts(ctx context.Context, limit int) ([]RunCost, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT run_id,
		       sum(cost_usd) AS total_cost_usd,
		       sum(input_tokens) AS total_input_tokens,
		       sum(output_tokens) AS total_output_tokens
		FROM spans
		GROUP BY run_id
		ORDER BY total_cost_usd DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []RunCost
	for rows.Next() {
		var r RunCost
		if err := rows.Scan(&r.RunID, &r.TotalCostUSD, &r.TotalInputTokens, &r.TotalOutputTokens); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

type SlowStep struct {
	StepType string  `json:"step_type"`
	ToolName string  `json:"tool_name"`
	P50      float64 `json:"p50_ms"`
	P95      float64 `json:"p95_ms"`
	P99      float64 `json:"p99_ms"`
	Total    uint64  `json:"total"`
}

// SlowSteps returns latency percentiles grouped by step type and tool.
func (q *Queries) SlowSteps(ctx context.Context) ([]SlowStep, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT step_type,
		       tool_name,
		       quantile(0.5)(latency_ms) AS p50,
		       quantile(0.95)(latency_ms) AS p95,
		       quantile(0.99)(latency_ms) AS p99,
		       count() AS total
		FROM spans
		GROUP BY step_type, tool_name
		ORDER BY p99 DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SlowStep
	for rows.Next() {
		var r SlowStep
		if err := rows.Scan(&r.StepType, &r.ToolName, &r.P50, &r.P95, &r.P99, &r.Total); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

type Loop struct {
	RunID     string `json:"run_id"`
	ToolName  string `json:"tool_name"`
	InputHash string `json:"input_hash"`
	Repeats   uint64 `json:"repeats"`
}

// Loops finds (run, tool, input_hash) combinations that occur at least
// minRepeats times within a run - a strong signal the agent got stuck
// calling the same tool with the same input over and over. This counts
// occurrences rather than requiring them to be consecutive: a run that
// calls the same tool with the same input 4+ times is stuck regardless of
// what else happens in between, and counting keeps the query simple.
func (q *Queries) Loops(ctx context.Context, minRepeats int) ([]Loop, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT run_id, tool_name, input_hash, count() AS repeats
		FROM spans
		WHERE step_type = 'tool_call' AND input_hash != ''
		GROUP BY run_id, tool_name, input_hash
		HAVING repeats >= ?
		ORDER BY repeats DESC
	`, minRepeats)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Loop
	for rows.Next() {
		var r Loop
		if err := rows.Scan(&r.RunID, &r.ToolName, &r.InputHash, &r.Repeats); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
