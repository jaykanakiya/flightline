package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/jaykanakiya/flightline-observability/internal/span"
)

type Inserter struct {
	conn clickhouse.Conn
}

func NewInserter(conn clickhouse.Conn) *Inserter {
	return &Inserter{conn: conn}
}

func (ins *Inserter) InsertSpans(ctx context.Context, spans []span.Span) error {
	batch, err := ins.conn.PrepareBatch(ctx, "INSERT INTO spans")
	if err != nil {
		return err
	}

	for _, s := range spans {
		err := batch.Append(
			s.TraceID,
			s.SpanID,
			s.ParentSpanID,
			s.AgentName,
			s.RunID,
			string(s.StepType),
			s.ToolName,
			s.Model,
			s.InputTokens,
			s.OutputTokens,
			s.CostUSD,
			s.LatencyMS,
			string(s.Status),
			s.ErrorMessage,
			s.InputHash,
			s.StartTime,
			s.EndTime,
			s.Attributes,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}
