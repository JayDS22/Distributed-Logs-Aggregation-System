package models_test

import (
	"testing"
	"time"

	"github.com/JayDS22/logstream/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestLogLevel_Valid(t *testing.T) {
	tests := []struct {
		level models.LogLevel
		want  bool
	}{
		{models.LevelDebug, true},
		{models.LevelInfo, true},
		{models.LevelWarn, true},
		{models.LevelError, true},
		{models.LevelFatal, true},
		{"TRACE", false},
		{"", false},
		{"info", false}, // case sensitive
	}
	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			assert.Equal(t, tt.want, tt.level.Valid())
		})
	}
}

func TestLogEvent_Validate(t *testing.T) {
	t.Run("valid event passes", func(t *testing.T) {
		ev := models.LogEvent{
			ServiceName: "api",
			SourceID:    "pod-1",
			Message:     "ok",
			Level:       models.LevelInfo,
		}
		assert.NoError(t, ev.Validate())
		assert.False(t, ev.Timestamp.IsZero(), "timestamp should be filled in")
	})

	t.Run("missing service_name fails", func(t *testing.T) {
		ev := models.LogEvent{SourceID: "x", Message: "m", Level: models.LevelInfo}
		assert.Error(t, ev.Validate())
	})

	t.Run("missing source_id fails", func(t *testing.T) {
		ev := models.LogEvent{ServiceName: "x", Message: "m", Level: models.LevelInfo}
		assert.Error(t, ev.Validate())
	})

	t.Run("missing message fails", func(t *testing.T) {
		ev := models.LogEvent{ServiceName: "x", SourceID: "y", Level: models.LevelInfo}
		assert.Error(t, ev.Validate())
	})

	t.Run("invalid level fails", func(t *testing.T) {
		ev := models.LogEvent{
			ServiceName: "x", SourceID: "y", Message: "m",
			Level: models.LogLevel("BOGUS"),
		}
		assert.Error(t, ev.Validate())
	})

	t.Run("preserves explicit timestamp", func(t *testing.T) {
		now := time.Now().Add(-1 * time.Hour)
		ev := models.LogEvent{
			ServiceName: "x", SourceID: "y", Message: "m",
			Level: models.LevelInfo, Timestamp: now,
		}
		assert.NoError(t, ev.Validate())
		assert.Equal(t, now, ev.Timestamp)
	})
}
