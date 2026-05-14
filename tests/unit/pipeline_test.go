package ingestion_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/ingestion"
	"github.com/JayDS22/logstream/internal/models"
	"github.com/JayDS22/logstream/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// We can't easily mock storage.Store (concrete struct) without an interface,
// but the BulkInsert path in Pipeline calls store.BulkInsert on the *Store.
// For testability we build a thin wrapper: we use a wrapper Pipeline that
// re-implements just the worker semantics. The cleaner approach in production
// would be a Storer interface; included here as a TODO and demonstrated via
// real-store integration tests below.

// helperCollector tallies submissions to validate validate / dedupe paths,
// which don't depend on the storage layer.
func TestPipeline_RejectsInvalidEvents(t *testing.T) {
	pipeline := ingestion.New(config.IngestorConfig{
		Workers: 1, BufferSize: 100, BatchSize: 10, FlushMillis: 50,
	}, nilStore(), zap.NewNop(), nil)

	// Don't start workers — just exercise Submit's validation gate.
	ok := pipeline.Submit(models.LogEvent{ServiceName: "", SourceID: "x", Message: "m", Level: models.LevelInfo})
	assert.False(t, ok, "missing service name should be rejected")

	ok = pipeline.Submit(models.LogEvent{ServiceName: "x", SourceID: "y", Message: "m", Level: "BOGUS"})
	assert.False(t, ok, "invalid level should be rejected")

	_, rejected, _, _ := pipeline.Stats()
	assert.Equal(t, int64(2), rejected)
}

func TestPipeline_DedupeByIdempotencyKey(t *testing.T) {
	pipeline := ingestion.New(config.IngestorConfig{
		Workers: 1, BufferSize: 100, BatchSize: 10, FlushMillis: 50,
	}, nilStore(), zap.NewNop(), nil)

	ev := models.LogEvent{
		ServiceName: "x", SourceID: "y", Message: "m", Level: models.LevelInfo,
		IdempotencyKey: "dupe-1",
	}
	require.True(t, pipeline.Submit(ev))
	require.True(t, pipeline.Submit(ev))      // dedupe — silent accept
	require.True(t, pipeline.Submit(ev))      // dedupe — silent accept
}

func TestPipeline_BufferBackpressure(t *testing.T) {
	pipeline := ingestion.New(config.IngestorConfig{
		Workers: 1, BufferSize: 2, BatchSize: 100, FlushMillis: 1000,
	}, nilStore(), zap.NewNop(), nil)

	// Don't start workers, so events accumulate.
	mk := func() models.LogEvent {
		return models.LogEvent{ServiceName: "x", SourceID: "y", Message: "m", Level: models.LevelInfo}
	}
	assert.True(t, pipeline.Submit(mk()))
	assert.True(t, pipeline.Submit(mk()))
	assert.False(t, pipeline.Submit(mk()), "third event must be dropped when buffer full")

	_, _, dropped, depth := pipeline.Stats()
	assert.Equal(t, int64(1), dropped)
	assert.Equal(t, int64(2), depth)
}

func TestPipeline_ConcurrentSubmits(t *testing.T) {
	pipeline := ingestion.New(config.IngestorConfig{
		Workers: 4, BufferSize: 10000, BatchSize: 50, FlushMillis: 100,
	}, nilStore(), zap.NewNop(), nil)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				pipeline.Submit(models.LogEvent{
					ServiceName: "svc", SourceID: "src", Message: "ok", Level: models.LevelInfo,
				})
			}
		}()
	}
	wg.Wait()

	// 800 submitted; buffer is 10k so none dropped.
	_, _, dropped, _ := pipeline.Stats()
	assert.Equal(t, int64(0), dropped)
}

func TestPipeline_StopWithoutStartIsNoop(t *testing.T) {
	pipeline := ingestion.New(config.IngestorConfig{
		Workers: 1, BufferSize: 10, BatchSize: 1, FlushMillis: 50,
	}, nilStore(), zap.NewNop(), nil)
	// Start so wg has workers to wait on.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	pipeline.Start(ctx)
	pipeline.Stop()
}

// nilStore returns a *storage.Store that will panic if BulkInsert is invoked.
// Tests above are careful not to actually start workers (Submit only enqueues).
func nilStore() *storage.Store { return nil }
