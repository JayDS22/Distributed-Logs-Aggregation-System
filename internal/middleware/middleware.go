package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/JayDS22/logstream/internal/metrics"
	"go.uber.org/zap"
)

// statusWriter captures the response status for metrics.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Recover catches panics and returns a 500. Always wrap routers with this.
func Recover(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic in handler",
						zap.Any("recover", rec),
						zap.String("path", r.URL.Path),
					)
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Metrics records request count and latency for every route.
func Metrics(c *metrics.Collectors) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: 200}
			next.ServeHTTP(sw, r)
			elapsed := time.Since(start).Seconds()
			c.RequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(sw.status)).Inc()
			c.RequestLatency.WithLabelValues(r.Method, r.URL.Path).Observe(elapsed)
		})
	}
}

// CORS allows the demo dashboard to call the API from a browser.
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
