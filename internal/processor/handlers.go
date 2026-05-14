package processor

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/JayDS22/logstream/internal/ingestion"
	"github.com/JayDS22/logstream/internal/models"
	"github.com/JayDS22/logstream/internal/storage"
	"go.uber.org/zap"
)

// Handlers holds dependencies for the HTTP API surface.
type Handlers struct {
	Pipeline *ingestion.Pipeline
	Store    *storage.Store
	Logger   *zap.Logger
	Started  time.Time
}

// Health returns 200 once dependencies are reachable.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.Store.Client().Ping(ctx, nil); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     "healthy",
		"uptime_sec": int(time.Since(h.Started).Seconds()),
	})
}

// Ready returns 200 once pipeline and storage are warm.
func (h *Handlers) Ready(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// Ingest accepts batches of log events.
func (h *Handlers) Ingest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req models.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if len(req.Events) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "events array is required"})
		return
	}
	if len(req.Events) > 10000 {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "batch too large (max 10000)"})
		return
	}
	accepted, rejected := h.Pipeline.SubmitBatch(req.Events)
	status := http.StatusAccepted
	if accepted == 0 && rejected > 0 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, models.IngestResponse{Accepted: accepted, Rejected: rejected})
}

// IngestSingle accepts a single log event (convenience endpoint).
func (h *Handlers) IngestSingle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var ev models.LogEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if h.Pipeline.Submit(ev) {
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "rejected"})
}

// Query searches logs by filter parameters.
func (h *Handlers) Query(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := models.QueryFilter{
		ServiceName: q.Get("service"),
		Level:       models.LogLevel(q.Get("level")),
		Search:      q.Get("q"),
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("since"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			filter.StartTime = time.Now().Add(-d)
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	events, err := h.Store.Query(ctx, filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"count":  len(events),
		"events": events,
	})
}

// Stats returns aggregated statistics over a configurable window.
func (h *Handlers) Stats(w http.ResponseWriter, r *http.Request) {
	window := 1 * time.Hour
	if v := r.URL.Query().Get("window"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			window = d
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	stats, err := h.Store.AggregateStats(ctx, time.Now().Add(-window))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// Throughput returns per-minute counts for charting.
func (h *Handlers) Throughput(w http.ResponseWriter, r *http.Request) {
	window := 1 * time.Hour
	if v := r.URL.Query().Get("window"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			window = d
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	buckets, err := h.Store.BucketByMinute(ctx, time.Now().Add(-window))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"buckets": buckets})
}

// PipelineStats exposes current ingestion pipeline counters.
func (h *Handlers) PipelineStats(w http.ResponseWriter, r *http.Request) {
	accepted, rejected, dropped, depth := h.Pipeline.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"accepted": accepted,
		"rejected": rejected,
		"dropped":  dropped,
		"depth":    depth,
	})
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
