// Package ingest holds the HTTP handler for POST /v1/spans. It validates
// and forwards spans to Redpanda - it never talks to ClickHouse directly,
// so a slow or down database doesn't block the agents sending traces.
package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/jaykanakiya/flightline-observability/internal/span"
)

type Producer interface {
	Send(ctx context.Context, messages []kafka.Message) error
}

type batchRequest struct {
	Spans []span.Span `json:"spans"`
}

func Handler(producer Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req batchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.Spans) == 0 {
			http.Error(w, "spans must not be empty", http.StatusBadRequest)
			return
		}

		messages := make([]kafka.Message, 0, len(req.Spans))
		for _, s := range req.Spans {
			if err := s.Validate(); err != nil {
				http.Error(w, "invalid span: "+err.Error(), http.StatusBadRequest)
				return
			}
			value, err := json.Marshal(s)
			if err != nil {
				http.Error(w, "encode error: "+err.Error(), http.StatusInternalServerError)
				return
			}
			messages = append(messages, kafka.Message{Key: []byte(s.SpanID), Value: value})
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := producer.Send(ctx, messages); err != nil {
			http.Error(w, "failed to enqueue spans: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]int{"accepted": len(req.Spans)})
	}
}
