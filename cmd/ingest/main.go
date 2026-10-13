package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jaykanakiya/flightline-observability/internal/config"
	"github.com/jaykanakiya/flightline-observability/internal/ingest"
	"github.com/jaykanakiya/flightline-observability/internal/kafka"
)

func main() {
	brokers := strings.Split(config.GetString("REDPANDA_BROKERS", "localhost:9092"), ",")
	topic := config.GetString("SPANS_TOPIC", "spans")
	addr := config.GetString("INGEST_ADDR", ":8080")

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/spans", ingest.Handler(producer))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("ingest listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
