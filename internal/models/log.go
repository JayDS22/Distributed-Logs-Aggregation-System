package models

import (
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// LogLevel represents the severity of a log event.
type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
	LevelFatal LogLevel = "FATAL"
)

// Valid returns whether the log level is one of the known severities.
func (l LogLevel) Valid() bool {
	switch l {
	case LevelDebug, LevelInfo, LevelWarn, LevelError, LevelFatal:
		return true
	}
	return false
}

// LogEvent represents a single log entry as stored in MongoDB.
// The MongoDB time-series collection uses Timestamp as the time field
// and ServiceName as the metaField.
type LogEvent struct {
	ID             primitive.ObjectID     `bson:"_id,omitempty" json:"id,omitempty"`
	Timestamp      time.Time              `bson:"timestamp" json:"timestamp"`
	ServiceName    string                 `bson:"service_name" json:"service_name"`
	SourceID       string                 `bson:"source_id" json:"source_id"`
	Level          LogLevel               `bson:"level" json:"level"`
	Message        string                 `bson:"message" json:"message"`
	TraceID        string                 `bson:"trace_id,omitempty" json:"trace_id,omitempty"`
	SpanID         string                 `bson:"span_id,omitempty" json:"span_id,omitempty"`
	Host           string                 `bson:"host,omitempty" json:"host,omitempty"`
	Environment    string                 `bson:"environment,omitempty" json:"environment,omitempty"`
	Tags           []string               `bson:"tags,omitempty" json:"tags,omitempty"`
	Fields         map[string]interface{} `bson:"fields,omitempty" json:"fields,omitempty"`
	IdempotencyKey string                 `bson:"idempotency_key,omitempty" json:"idempotency_key,omitempty"`
}

// Validate performs schema validation on a log event before ingestion.
func (e *LogEvent) Validate() error {
	if e.ServiceName == "" {
		return errors.New("service_name is required")
	}
	if e.SourceID == "" {
		return errors.New("source_id is required")
	}
	if e.Message == "" {
		return errors.New("message is required")
	}
	if !e.Level.Valid() {
		return errors.New("invalid level: must be DEBUG, INFO, WARN, ERROR, or FATAL")
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	return nil
}

// IngestRequest is the wire format accepted by the ingestion HTTP endpoint.
type IngestRequest struct {
	Events []LogEvent `json:"events"`
}

// IngestResponse is returned to clients after ingestion.
type IngestResponse struct {
	Accepted int      `json:"accepted"`
	Rejected int      `json:"rejected"`
	Errors   []string `json:"errors,omitempty"`
}

// QueryFilter represents filters used by the query API.
type QueryFilter struct {
	ServiceName string    `json:"service_name,omitempty"`
	Level       LogLevel  `json:"level,omitempty"`
	StartTime   time.Time `json:"start_time,omitempty"`
	EndTime     time.Time `json:"end_time,omitempty"`
	Search      string    `json:"search,omitempty"`
	Limit       int64     `json:"limit,omitempty"`
}

// AlertRule defines a pattern that triggers an alert when matched.
type AlertRule struct {
	ID          string   `bson:"_id" json:"id"`
	Name        string   `bson:"name" json:"name"`
	ServiceName string   `bson:"service_name,omitempty" json:"service_name,omitempty"`
	Level       LogLevel `bson:"level,omitempty" json:"level,omitempty"`
	Pattern     string   `bson:"pattern,omitempty" json:"pattern,omitempty"`
	Threshold   int      `bson:"threshold" json:"threshold"`
	WindowSecs  int      `bson:"window_secs" json:"window_secs"`
	Channel     string   `bson:"channel" json:"channel"` // slack|pagerduty|webhook
	Webhook     string   `bson:"webhook,omitempty" json:"webhook,omitempty"`
	Enabled     bool     `bson:"enabled" json:"enabled"`
}
