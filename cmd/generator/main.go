// generator pushes synthetic agent runs to the ingest service at a
// configurable rate, up to a configurable total. It's used both to fill
// the dashboard with realistic-looking data (Stage 2) and as the load
// generator for the benchmark (Stage 3).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/jaykanakiya/flightline-observability/internal/config"
	"github.com/jaykanakiya/flightline-observability/internal/span"
)

type batchRequest struct {
	Spans []span.Span `json:"spans"`
}

func main() {
	// Flags default to the env vars docker-compose sets, but can still be
	// overridden on the command line - e.g. for the Stage 3 load test.
	ingestURL := flag.String("ingest-url", config.GetString("INGEST_URL", "http://localhost:8080/v1/spans"), "ingest endpoint")
	targetRate := flag.Int("rate", config.GetInt("GEN_RATE", 500), "target spans per second")
	total := flag.Int("total", config.GetInt("GEN_TOTAL", 20000), "total spans to send")
	batchSize := flag.Int("batch-size", config.GetInt("GEN_BATCH_SIZE", 500), "spans per HTTP request")
	concurrency := flag.Int("concurrency", config.GetInt("GEN_CONCURRENCY", 8), "number of concurrent senders")
	flag.Parse()

	limiter := rate.NewLimiter(rate.Limit(*targetRate), *batchSize)
	client := &http.Client{Timeout: 10 * time.Second}

	var sent int64
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			worker(rand.New(rand.NewSource(seed)), client, limiter, *ingestURL, *batchSize, *total, &sent)
		}(start.UnixNano() + int64(w))
	}
	wg.Wait()

	elapsed := time.Since(start)
	n := atomic.LoadInt64(&sent)
	log.Printf("sent %d spans in %s (%.0f spans/sec)", n, elapsed, float64(n)/elapsed.Seconds())
}

func worker(rng *rand.Rand, client *http.Client, limiter *rate.Limiter, ingestURL string, batchSize, total int, sent *int64) {
	var buf []span.Span

	for atomic.LoadInt64(sent) < int64(total) {
		buf = append(buf, generateRun(rng)...)

		for len(buf) >= batchSize {
			batch := buf[:batchSize]
			buf = buf[batchSize:]
			sendBatch(client, limiter, ingestURL, batch, sent)
		}
	}

	if len(buf) > 0 {
		sendBatch(client, limiter, ingestURL, buf, sent)
	}
}

func sendBatch(client *http.Client, limiter *rate.Limiter, ingestURL string, batch []span.Span, sent *int64) {
	if err := limiter.WaitN(context.Background(), len(batch)); err != nil {
		log.Printf("limiter error: %v", err)
		return
	}

	body, err := json.Marshal(batchRequest{Spans: batch})
	if err != nil {
		log.Printf("encode error: %v", err)
		return
	}

	resp, err := client.Post(ingestURL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("send error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		log.Printf("ingest returned %s", resp.Status)
		return
	}

	atomic.AddInt64(sent, int64(len(batch)))
}
