// Package span defines the Span record that flows through the whole
// pipeline: ingest -> Redpanda -> consumer -> ClickHouse.
package span

import (
	"fmt"
	"time"
)

type StepType string

const (
	StepLLMCall   StepType = "llm_call"
	StepToolCall  StepType = "tool_call"
	StepAgentStep StepType = "agent_step"
)

type Status string

const (
	StatusOK      Status = "ok"
	StatusError   Status = "error"
	StatusTimeout Status = "timeout"
)

type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	AgentName    string            `json:"agent_name"`
	RunID        string            `json:"run_id"`
	StepType     StepType          `json:"step_type"`
	ToolName     string            `json:"tool_name,omitempty"`
	Model        string            `json:"model,omitempty"`
	InputTokens  uint32            `json:"input_tokens,omitempty"`
	OutputTokens uint32            `json:"output_tokens,omitempty"`
	CostUSD      float64           `json:"cost_usd,omitempty"`
	LatencyMS    uint32            `json:"latency_ms"`
	Status       Status            `json:"status"`
	ErrorMessage string            `json:"error_message,omitempty"`
	InputHash    string            `json:"input_hash,omitempty"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

// Validate checks the fields required for the span to be queryable and
// storable. It doesn't try to validate everything (e.g. tool_name is only
// meaningful for tool_call spans) - just enough to catch garbage early.
func (s Span) Validate() error {
	if s.TraceID == "" {
		return fmt.Errorf("trace_id is required")
	}
	if s.SpanID == "" {
		return fmt.Errorf("span_id is required")
	}
	if s.RunID == "" {
		return fmt.Errorf("run_id is required")
	}

	switch s.StepType {
	case StepLLMCall, StepToolCall, StepAgentStep:
	default:
		return fmt.Errorf("invalid step_type: %q", s.StepType)
	}

	switch s.Status {
	case StatusOK, StatusError, StatusTimeout:
	default:
		return fmt.Errorf("invalid status: %q", s.Status)
	}

	if s.StartTime.IsZero() {
		return fmt.Errorf("start_time is required")
	}
	if s.EndTime.Before(s.StartTime) {
		return fmt.Errorf("end_time is before start_time")
	}

	return nil
}
