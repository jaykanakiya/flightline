package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaykanakiya/flightline-observability/internal/api"
	chclient "github.com/jaykanakiya/flightline-observability/internal/clickhouse"
	"github.com/jaykanakiya/flightline-observability/internal/config"
)

func main() {
	addr := config.GetString("API_ADDR", ":8081")
	chAddr := config.GetString("CLICKHOUSE_ADDR", "localhost:9000")
	chDB := config.GetString("CLICKHOUSE_DB", "default")
	chUser := config.GetString("CLICKHOUSE_USER", "default")
	chPass := config.GetString("CLICKHOUSE_PASSWORD", "")

	conn, err := chclient.NewConn(chAddr, chDB, chUser, chPass)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	queries := chclient.NewQueries(conn)

	srv := &http.Server{Addr: addr, Handler: api.NewRouter(queries)}

	go func() {
		log.Printf("api listening on %s", addr)
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
