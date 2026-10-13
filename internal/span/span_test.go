package span

import (
	"testing"
	"time"
)

func validSpan() Span {
	now := time.Now()
	return Span{
		TraceID:   "trace-1",
		SpanID:    "span-1",
		RunID:     "run-1",
		StepType:  StepToolCall,
		Status:    StatusOK,
		StartTime: now,
		EndTime:   now.Add(10 * time.Millisecond),
	}
}

func TestValidate_OK(t *testing.T) {
	if err := validSpan().Validate(); err != nil {
		t.Fatalf("expected valid span, got error: %v", err)
	}
}

func TestValidate_MissingRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Span)
	}{
		{"missing trace_id", func(s *Span) { s.TraceID = "" }},
		{"missing span_id", func(s *Span) { s.SpanID = "" }},
		{"missing run_id", func(s *Span) { s.RunID = "" }},
		{"missing start_time", func(s *Span) { s.StartTime = time.Time{} }},
		{"bad step_type", func(s *Span) { s.StepType = "not_a_step" }},
		{"bad status", func(s *Span) { s.Status = "not_a_status" }},
		{"end before start", func(s *Span) { s.EndTime = s.StartTime.Add(-time.Second) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := validSpan()
			tc.mutate(&s)
			if err := s.Validate(); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}
