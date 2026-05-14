// Command loadgen generates synthetic log traffic against the ingestion
// endpoint. Useful for local throughput testing.
//
// Usage:
//
//	go run ./cmd/loadgen -url http://localhost:8080/api/v1/logs -rps 5000 -duration 30s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JayDS22/logstream/internal/models"
	"github.com/google/uuid"
)

var (
	services = []string{"api-gateway", "user-service", "order-service", "payment-service", "inventory-service", "notification-service", "auth-service", "search-service"}
	levels   = []models.LogLevel{models.LevelInfo, models.LevelInfo, models.LevelInfo, models.LevelInfo, models.LevelWarn, models.LevelError, models.LevelDebug}
	messages = []string{
		"request completed successfully",
		"cache miss for key user:42",
		"connection pool exhausted, waiting",
		"slow query detected (320ms)",
		"failed to acquire distributed lock",
		"retrying upstream call (attempt 2)",
		"payment gateway returned 502",
		"user authenticated",
		"order placed",
		"inventory check passed",
	}
)

func randomEvent() models.LogEvent {
	return models.LogEvent{
		Timestamp:   time.Now().UTC(),
		ServiceName: services[rand.Intn(len(services))],
		SourceID:    fmt.Sprintf("pod-%d", rand.Intn(100)),
		Level:       levels[rand.Intn(len(levels))],
		Message:     messages[rand.Intn(len(messages))],
		TraceID:     uuid.NewString(),
		Host:        fmt.Sprintf("host-%d", rand.Intn(20)),
		Environment: "production",
		Tags:        []string{"region:us-east-1"},
	}
}

func main() {
	url := flag.String("url", "http://localhost:8080/api/v1/logs", "ingestion URL")
	rps := flag.Int("rps", 1000, "target requests per second")
	batch := flag.Int("batch", 50, "events per request")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	workers := flag.Int("workers", 16, "concurrent workers")
	flag.Parse()

	client := &http.Client{Timeout: 5 * time.Second}
	var sent, accepted, errs atomic.Int64

	stop := make(chan struct{})
	var wg sync.WaitGroup

	rate := time.Duration(int64(time.Second) / int64(*rps) * int64(*batch))
	if rate < time.Microsecond {
		rate = time.Microsecond
	}

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(rate * time.Duration(*workers))
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					events := make([]models.LogEvent, *batch)
					for i := range events {
						events[i] = randomEvent()
					}
					body, _ := json.Marshal(models.IngestRequest{Events: events})
					sent.Add(int64(*batch))
					resp, err := client.Post(*url, "application/json", bytes.NewReader(body))
					if err != nil {
						errs.Add(1)
						continue
					}
					var r models.IngestResponse
					_ = json.NewDecoder(resp.Body).Decode(&r)
					accepted.Add(int64(r.Accepted))
					resp.Body.Close()
				}
			}
		}()
	}

	start := time.Now()
	report := time.NewTicker(2 * time.Second)
	defer report.Stop()
	deadline := time.After(*duration)

LOOP:
	for {
		select {
		case <-deadline:
			break LOOP
		case <-report.C:
			elapsed := time.Since(start).Seconds()
			s := sent.Load()
			a := accepted.Load()
			fmt.Printf("[%4.1fs] sent=%d accepted=%d errors=%d throughput=%.0f eps\n",
				elapsed, s, a, errs.Load(), float64(s)/elapsed)
		}
	}
	close(stop)
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	fmt.Println("==== load test complete ====")
	fmt.Printf("duration:    %.2fs\n", elapsed)
	fmt.Printf("sent:        %d\n", sent.Load())
	fmt.Printf("accepted:    %d\n", accepted.Load())
	fmt.Printf("errors:      %d\n", errs.Load())
	fmt.Printf("throughput:  %.0f events/sec\n", float64(sent.Load())/elapsed)
	if errs.Load() > 0 {
		os.Exit(1)
	}
}
