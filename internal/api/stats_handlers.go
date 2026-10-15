package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jaykanakiya/flightline-observability/internal/clickhouse"
)

func toolFailuresHandler(q *clickhouse.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hours := intParam(r, "hours", 24)
		results, err := q.ToolFailures(r.Context(), hours)
		writeJSON(w, results, err)
	}
}

func runCostsHandler(q *clickhouse.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := intParam(r, "limit", 20)
		results, err := q.RunCosts(r.Context(), limit)
		writeJSON(w, results, err)
	}
}

func slowStepsHandler(q *clickhouse.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results, err := q.SlowSteps(r.Context())
		writeJSON(w, results, err)
	}
}

func loopsHandler(q *clickhouse.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		minRepeats := intParam(r, "min_repeats", 4)
		results, err := q.Loops(r.Context(), minRepeats)
		writeJSON(w, results, err)
	}
}

func intParam(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
