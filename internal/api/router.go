// Package api exposes the four stats endpoints as plain JSON HTTP
// handlers, each backed by one ClickHouse query.
package api

import (
	"net/http"

	"github.com/jaykanakiya/flightline-observability/internal/clickhouse"
)

func NewRouter(q *clickhouse.Queries) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/stats/tool-failures", toolFailuresHandler(q))
	mux.HandleFunc("GET /v1/stats/run-costs", runCostsHandler(q))
	mux.HandleFunc("GET /v1/stats/slow-steps", slowStepsHandler(q))
	mux.HandleFunc("GET /v1/stats/loops", loopsHandler(q))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
