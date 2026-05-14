// Package integration contains tests that exercise the real MongoDB driver
// against a running mongod. They are guarded by the MONGODB_TEST_URI env var
// so `go test ./...` works without infrastructure.
//
// Example:
//
//	MONGODB_TEST_URI=mongodb://localhost:27017 go test ./tests/integration/...
package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/metrics"
	"github.com/JayDS22/logstream/internal/models"
	"github.com/JayDS22/logstream/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func mongoURI(t *testing.T) string {
	uri := os.Getenv("MONGODB_TEST_URI")
	if uri == "" {
		t.Skip("MONGODB_TEST_URI not set; skipping integration test")
	}
	return uri
}

func newStore(t *testing.T) *storage.Store {
	t.Helper()
	cfg := &config.MongoDBConfig{
		URI:             mongoURI(t),
		Database:        "logstream_test_" + time.Now().Format("150405"),
		Collection:      "logs",
		RulesCollection: "alert_rules",
		ConnectTimeout:  5 * time.Second,
		MaxPoolSize:     20,
		MinPoolSize:     1,
		TimeSeries:      false, // tests use a normal collection (simpler cleanup)
	}
	ctx := context.Background()
	store, err := storage.New(ctx, cfg, zap.NewNop(), metrics.New())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Client().Database(cfg.Database).Drop(ctx)
		_ = store.Close(ctx)
	})
	return store
}

func TestIntegration_BulkInsertAndQuery(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	events := []models.LogEvent{
		{Timestamp: time.Now(), ServiceName: "api", SourceID: "pod-1", Level: models.LevelInfo, Message: "ok"},
		{Timestamp: time.Now(), ServiceName: "api", SourceID: "pod-2", Level: models.LevelError, Message: "boom"},
		{Timestamp: time.Now(), ServiceName: "worker", SourceID: "pod-3", Level: models.LevelInfo, Message: "tick"},
	}
	n, err := store.BulkInsert(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	all, err := store.Query(ctx, models.QueryFilter{Limit: 10})
	require.NoError(t, err)
	assert.Len(t, all, 3)

	errOnly, err := store.Query(ctx, models.QueryFilter{Level: models.LevelError, Limit: 10})
	require.NoError(t, err)
	assert.Len(t, errOnly, 1)
	assert.Equal(t, "boom", errOnly[0].Message)

	apiOnly, err := store.Query(ctx, models.QueryFilter{ServiceName: "api", Limit: 10})
	require.NoError(t, err)
	assert.Len(t, apiOnly, 2)
}

func TestIntegration_AggregateStats(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	now := time.Now()
	events := []models.LogEvent{
		{Timestamp: now, ServiceName: "api", SourceID: "p1", Level: models.LevelInfo, Message: "a"},
		{Timestamp: now, ServiceName: "api", SourceID: "p1", Level: models.LevelInfo, Message: "b"},
		{Timestamp: now, ServiceName: "api", SourceID: "p1", Level: models.LevelError, Message: "c"},
		{Timestamp: now, ServiceName: "worker", SourceID: "p2", Level: models.LevelWarn, Message: "d"},
	}
	_, err := store.BulkInsert(ctx, events)
	require.NoError(t, err)

	stats, err := store.AggregateStats(ctx, now.Add(-1*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(4), stats.TotalEvents)
	assert.Equal(t, int64(2), stats.ByLevel["INFO"])
	assert.Equal(t, int64(1), stats.ByLevel["ERROR"])
	assert.InDelta(t, 0.25, stats.ErrorRate, 0.001)
	assert.Equal(t, int64(3), stats.ByService["api"])
}
