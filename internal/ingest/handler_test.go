package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fakeProducer struct {
	sent []kafka.Message
	err  error
}

func (f *fakeProducer) Send(ctx context.Context, messages []kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, messages...)
	return nil
}

func post(t *testing.T, producer Producer, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/spans", strings.NewReader(body))
	rec := httptest.NewRecorder()
	Handler(producer).ServeHTTP(rec, req)
	return rec
}

func TestHandler_AcceptsValidBatch(t *testing.T) {
	producer := &fakeProducer{}
	body := `{"spans": [{
		"trace_id": "t1", "span_id": "s1", "run_id": "r1",
		"step_type": "tool_call", "status": "ok",
		"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T00:00:01Z"
	}]}`

	rec := post(t, producer, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(producer.sent) != 1 {
		t.Fatalf("expected 1 message sent, got %d", len(producer.sent))
	}
}

func TestHandler_RejectsInvalidSpan(t *testing.T) {
	producer := &fakeProducer{}
	body := `{"spans": [{"trace_id": "t1"}]}` // missing span_id, run_id, etc.

	rec := post(t, producer, body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if len(producer.sent) != 0 {
		t.Fatal("no message should be sent for an invalid batch")
	}
}

func TestHandler_RejectsEmptyBatch(t *testing.T) {
	rec := post(t, &fakeProducer{}, `{"spans": []}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandler_RejectsBadJSON(t *testing.T) {
	rec := post(t, &fakeProducer{}, `not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
