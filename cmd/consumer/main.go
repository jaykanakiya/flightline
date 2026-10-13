package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/jaykanakiya/flightline-observability/internal/batch"
	chclient "github.com/jaykanakiya/flightline-observability/internal/clickhouse"
	"github.com/jaykanakiya/flightline-observability/internal/config"
	"github.com/jaykanakiya/flightline-observability/internal/kafka"
	"github.com/jaykanakiya/flightline-observability/internal/span"
)

// item pairs a decoded span with the raw kafka message it came from, so we
// can commit the right offsets once the batch is safely in ClickHouse.
type item struct {
	msg kafkago.Message
	sp  span.Span
}

func main() {
	brokers := strings.Split(config.GetString("REDPANDA_BROKERS", "localhost:9092"), ",")
	topic := config.GetString("SPANS_TOPIC", "spans")
	groupID := config.GetString("CONSUMER_GROUP", "spans-consumer")
	chAddr := config.GetString("CLICKHOUSE_ADDR", "localhost:9000")
	chDB := config.GetString("CLICKHOUSE_DB", "default")
	chUser := config.GetString("CLICKHOUSE_USER", "default")
	chPass := config.GetString("CLICKHOUSE_PASSWORD", "")
	maxBatch := config.GetInt("BATCH_MAX_SIZE", 10000)
	maxWaitMS := config.GetInt("BATCH_MAX_WAIT_MS", 1000)

	conn, err := chclient.NewConn(chAddr, chDB, chUser, chPass)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	inserter := chclient.NewInserter(conn)

	consumer := kafka.NewConsumer(brokers, topic, groupID)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("shutting down")
		cancel()
	}()

	collector := batch.NewCollector[item](maxBatch, time.Duration(maxWaitMS)*time.Millisecond)

	for {
		if ctx.Err() != nil {
			flush(context.Background(), consumer, inserter, collector)
			return
		}

		fetchCtx, fetchCancel := context.WithTimeout(ctx, collector.TimeLeft())
		msg, err := consumer.FetchMessage(fetchCtx)
		fetchCancel()

		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				log.Printf("fetch error: %v", err)
			}
			if collector.Ready() {
				flush(context.Background(), consumer, inserter, collector)
			}
			continue
		}

		var sp span.Span
		if err := json.Unmarshal(msg.Value, &sp); err != nil {
			log.Printf("skipping unparseable message: %v", err)
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit of skipped message failed: %v", err)
			}
			continue
		}

		if collector.Add(item{msg: msg, sp: sp}) {
			flush(context.Background(), consumer, inserter, collector)
		}
	}
}

// flush inserts the current batch and commits its offsets. If the insert
// fails we exit without committing: Redpanda redelivers the batch from the
// last committed offset after a restart, giving at-least-once delivery
// instead of a half-written batch with a silently advanced offset.
func flush(ctx context.Context, consumer *kafka.Consumer, inserter *chclient.Inserter, collector *batch.Collector[item]) {
	items := collector.Take()
	if len(items) == 0 {
		return
	}

	spans := make([]span.Span, len(items))
	messages := make([]kafkago.Message, len(items))
	for i, it := range items {
		spans[i] = it.sp
		messages[i] = it.msg
	}

	if err := inserter.InsertSpans(ctx, spans); err != nil {
		log.Fatalf("insert failed, exiting so this batch is redelivered: %v", err)
	}

	if err := consumer.CommitMessages(ctx, messages...); err != nil {
		log.Fatalf("commit failed: %v", err)
	}

	log.Printf("inserted %d spans", len(spans))
}
