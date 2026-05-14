package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/metrics"
	"github.com/JayDS22/logstream/internal/models"
	"github.com/JayDS22/logstream/internal/storage"
	"go.uber.org/zap"
)

// Pipeline is a goroutine-based concurrent ingestion pipeline. It accepts
// log events on an unbounded-looking buffered channel, dedups by idempotency
// key, batches by size or time, and writes to MongoDB via bulk inserts.
//
// Backpressure: when the buffer is full, Submit returns false and the caller
// (HTTP handler / Kafka consumer) is expected to reject the event with a 503.
type Pipeline struct {
	cfg        config.IngestorConfig
	store      *storage.Store
	logger     *zap.Logger
	collectors *metrics.Collectors

	in chan models.LogEvent
	wg sync.WaitGroup

	// dedupe: lightweight in-memory ring of seen idempotency keys.
	dedupeMu sync.Mutex
	dedupe   map[string]int64
	dedupeAt int64

	// stats (atomic counters for lock-free fast-path)
	accepted atomic.Int64
	rejected atomic.Int64
	dropped  atomic.Int64
}

// New constructs a Pipeline. Call Start to launch workers.
func New(cfg config.IngestorConfig, store *storage.Store, log *zap.Logger, m *metrics.Collectors) *Pipeline {
	return &Pipeline{
		cfg:        cfg,
		store:      store,
		logger:     log,
		collectors: m,
		in:         make(chan models.LogEvent, cfg.BufferSize),
		dedupe:     make(map[string]int64, 4096),
	}
}

// Start launches the worker pool. Workers run until ctx is canceled.
func (p *Pipeline) Start(ctx context.Context) {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	// metrics scraper: queue depth gauge
	if p.collectors != nil {
		p.wg.Add(1)
		go p.depthScraper(ctx)
	}
	p.logger.Info("ingestion pipeline started",
		zap.Int("workers", p.cfg.Workers),
		zap.Int("buffer", p.cfg.BufferSize),
		zap.Int("batch_size", p.cfg.BatchSize),
	)
}

// Submit attempts a non-blocking enqueue. Returns false if buffer is full.
func (p *Pipeline) Submit(ev models.LogEvent) bool {
	if err := ev.Validate(); err != nil {
		p.rejected.Add(1)
		if p.collectors != nil {
			p.collectors.RejectedTotal.Inc()
		}
		return false
	}
	if !p.checkAndStoreDedup(ev.IdempotencyKey) {
		// Duplicate; treat as accepted for at-least-once semantics.
		return true
	}
	select {
	case p.in <- ev:
		return true
	default:
		p.dropped.Add(1)
		return false
	}
}

// SubmitBatch attempts to submit many events. Returns (accepted, rejected).
func (p *Pipeline) SubmitBatch(events []models.LogEvent) (int, int) {
	var ok, bad int
	for _, e := range events {
		if p.Submit(e) {
			ok++
		} else {
			bad++
		}
	}
	return ok, bad
}

// Stop gracefully drains the pipeline and waits for workers to finish.
func (p *Pipeline) Stop() {
	close(p.in)
	p.wg.Wait()
	p.logger.Info("ingestion pipeline stopped",
		zap.Int64("accepted", p.accepted.Load()),
		zap.Int64("rejected", p.rejected.Load()),
		zap.Int64("dropped", p.dropped.Load()),
	)
}

// Stats returns a snapshot of pipeline counters.
func (p *Pipeline) Stats() (accepted, rejected, dropped, depth int64) {
	return p.accepted.Load(), p.rejected.Load(), p.dropped.Load(), int64(len(p.in))
}

func (p *Pipeline) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	batch := make([]models.LogEvent, 0, p.cfg.BatchSize)
	flushTick := time.NewTicker(time.Duration(p.cfg.FlushMillis) * time.Millisecond)
	defer flushTick.Stop()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		inserted, err := p.store.BulkInsert(flushCtx, batch)
		cancel()
		if err != nil {
			p.logger.Error("bulk insert failed",
				zap.Int("worker", id),
				zap.String("reason", reason),
				zap.Int("batch", len(batch)),
				zap.Error(err),
			)
		}
		p.accepted.Add(int64(inserted))
		if p.collectors != nil {
			p.collectors.IngestedTotal.Add(float64(inserted))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush("shutdown")
			return
		case ev, ok := <-p.in:
			if !ok {
				flush("channel-closed")
				return
			}
			batch = append(batch, ev)
			if len(batch) >= p.cfg.BatchSize {
				flush("size")
			}
		case <-flushTick.C:
			flush("interval")
		}
	}
}

func (p *Pipeline) depthScraper(ctx context.Context) {
	defer p.wg.Done()
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.collectors.QueueDepth.Set(float64(len(p.in)))
		}
	}
}

// checkAndStoreDedup returns true if the key is new (should be processed).
// Empty keys always pass through (no dedupe requested).
func (p *Pipeline) checkAndStoreDedup(key string) bool {
	if key == "" {
		return true
	}
	now := time.Now().Unix()
	p.dedupeMu.Lock()
	defer p.dedupeMu.Unlock()

	// Sweep the cache every ~60s to avoid unbounded growth.
	if now-p.dedupeAt > 60 {
		cutoff := now - 300 // 5-minute dedupe window
		for k, t := range p.dedupe {
			if t < cutoff {
				delete(p.dedupe, k)
			}
		}
		p.dedupeAt = now
	}

	if _, seen := p.dedupe[key]; seen {
		return false
	}
	p.dedupe[key] = now
	return true
}
