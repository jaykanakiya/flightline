package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/jaykanakiya/flightline-observability/internal/span"
)

// These tests need a real ClickHouse (e.g. `docker compose up clickhouse`)
// and skip cleanly if one isn't reachable, so `go test ./...` still passes
// without Docker running.
func testConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	conn, err := NewConn("localhost:9000", "default", "default", "flightline")
	if err != nil {
		t.Skipf("clickhouse not reachable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Skipf("clickhouse not reachable: %v", err)
	}
	return conn
}

func TestLoops_DetectsStuckRunButNotNormalOne(t *testing.T) {
	conn := testConn(t)
	ctx := context.Background()
	inserter := NewInserter(conn)
	queries := NewQueries(conn)

	stuckRun := uuid.NewString()
	normalRun := uuid.NewString()
	now := time.Now()

	var spans []span.Span
	for i := 0; i < 5; i++ {
		spans = append(spans, span.Span{
			TraceID:   stuckRun,
			SpanID:    uuid.NewString(),
			RunID:     stuckRun,
			StepType:  span.StepToolCall,
			ToolName:  "web_search",
			InputHash: "same-input",
			Status:    span.StatusOK,
			StartTime: now,
			EndTime:   now,
		})
	}
	for i := 0; i < 2; i++ {
		spans = append(spans, span.Span{
			TraceID:   normalRun,
			SpanID:    uuid.NewString(),
			RunID:     normalRun,
			StepType:  span.StepToolCall,
			ToolName:  "web_search",
			InputHash: "same-input",
			Status:    span.StatusOK,
			StartTime: now,
			EndTime:   now,
		})
	}

	if err := inserter.InsertSpans(ctx, spans); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	results, err := queries.Loops(ctx, 4)
	if err != nil {
		t.Fatalf("loops query failed: %v", err)
	}

	var found *Loop
	for _, r := range results {
		if r.RunID == stuckRun {
			found = &r
		}
		if r.RunID == normalRun {
			t.Fatalf("normal run with only 2 repeats should not be reported as a loop: %+v", r)
		}
	}
	if found == nil {
		t.Fatal("expected the stuck run to be reported as a loop")
	}
	if found.Repeats != 5 {
		t.Fatalf("expected 5 repeats, got %d", found.Repeats)
	}
}
