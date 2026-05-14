package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/metrics"
	"github.com/JayDS22/logstream/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// Store is the MongoDB-backed persistence layer for log events.
type Store struct {
	client     *mongo.Client
	db         *mongo.Database
	logs       *mongo.Collection
	rules      *mongo.Collection
	cfg        *config.MongoDBConfig
	logger     *zap.Logger
	collectors *metrics.Collectors
}

// New connects to MongoDB and prepares the time-series collection and indexes.
func New(ctx context.Context, cfg *config.MongoDBConfig, log *zap.Logger, m *metrics.Collectors) (*Store, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetRetryWrites(true)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	db := client.Database(cfg.Database)
	s := &Store{
		client:     client,
		db:         db,
		logs:       db.Collection(cfg.Collection),
		rules:      db.Collection(cfg.RulesCollection),
		cfg:        cfg,
		logger:     log,
		collectors: m,
	}

	if err := s.ensureCollections(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// ensureCollections creates the time-series collection if it does not exist.
func (s *Store) ensureCollections(ctx context.Context) error {
	names, err := s.db.ListCollectionNames(ctx, bson.M{"name": s.cfg.Collection})
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}
	if len(names) > 0 {
		return nil
	}

	if !s.cfg.TimeSeries {
		return s.db.CreateCollection(ctx, s.cfg.Collection)
	}

	tsOpts := options.TimeSeries().
		SetTimeField("timestamp").
		SetMetaField("service_name").
		SetGranularity("seconds")

	createOpts := options.CreateCollection().SetTimeSeriesOptions(tsOpts)
	if s.cfg.TTLDays > 0 {
		expire := int64(s.cfg.TTLDays) * 24 * 60 * 60
		createOpts.SetExpireAfterSeconds(expire)
	}

	if err := s.db.CreateCollection(ctx, s.cfg.Collection, createOpts); err != nil {
		// CommandError code 48 = NamespaceExists; tolerate races on startup.
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) && cmdErr.Code == 48 {
			return nil
		}
		return fmt.Errorf("create time-series collection: %w", err)
	}
	s.logger.Info("created time-series collection", zap.String("name", s.cfg.Collection))
	return nil
}

// ensureIndexes builds the compound indexes used by query and alert paths.
func (s *Store) ensureIndexes(ctx context.Context) error {
	idx := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "timestamp", Value: -1},
				{Key: "level", Value: 1},
				{Key: "service_name", Value: 1},
			},
			Options: options.Index().SetName("ts_level_service"),
		},
		{
			Keys: bson.D{
				{Key: "service_name", Value: 1},
				{Key: "timestamp", Value: -1},
			},
			Options: options.Index().SetName("service_ts"),
		},
		{
			Keys:    bson.D{{Key: "trace_id", Value: 1}},
			Options: options.Index().SetName("trace_id").SetSparse(true),
		},
	}

	if _, err := s.logs.Indexes().CreateMany(ctx, idx); err != nil {
		// Time-series collections in older MongoDB versions reject some index
		// kinds; downgrade these to warnings rather than fatal errors.
		s.logger.Warn("index creation partial", zap.Error(err))
	}
	return nil
}

// BulkInsert performs an unordered bulk insert for maximum throughput.
// Returns the number of successfully inserted documents.
func (s *Store) BulkInsert(ctx context.Context, events []models.LogEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	docs := make([]interface{}, len(events))
	for i := range events {
		docs[i] = events[i]
	}

	start := time.Now()
	opts := options.InsertMany().SetOrdered(false)
	res, err := s.logs.InsertMany(ctx, docs, opts)
	elapsed := time.Since(start).Seconds()

	if s.collectors != nil {
		s.collectors.WriteLatency.Observe(elapsed)
		s.collectors.BatchSize.Observe(float64(len(events)))
	}

	if err != nil {
		// Partial success: BulkWriteException returns inserted IDs.
		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) {
			inserted := len(events) - len(bwe.WriteErrors)
			if s.collectors != nil {
				s.collectors.WriteErrors.Add(float64(len(bwe.WriteErrors)))
			}
			return inserted, nil
		}
		if s.collectors != nil {
			s.collectors.WriteErrors.Inc()
		}
		return 0, err
	}
	return len(res.InsertedIDs), nil
}

// Query returns logs matching the filter, newest first.
func (s *Store) Query(ctx context.Context, f models.QueryFilter) ([]models.LogEvent, error) {
	filter := bson.M{}
	if f.ServiceName != "" {
		filter["service_name"] = f.ServiceName
	}
	if f.Level != "" {
		filter["level"] = f.Level
	}
	if !f.StartTime.IsZero() || !f.EndTime.IsZero() {
		ts := bson.M{}
		if !f.StartTime.IsZero() {
			ts["$gte"] = f.StartTime
		}
		if !f.EndTime.IsZero() {
			ts["$lte"] = f.EndTime
		}
		filter["timestamp"] = ts
	}
	if f.Search != "" {
		filter["message"] = bson.M{"$regex": f.Search, "$options": "i"}
	}

	limit := f.Limit
	if limit <= 0 || limit > 5000 {
		limit = 200
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(limit)

	cur, err := s.logs.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []models.LogEvent
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Stats represents a snapshot of aggregated log statistics over a window.
type Stats struct {
	TotalEvents int64            `json:"total_events"`
	ByLevel     map[string]int64 `json:"by_level"`
	ByService   map[string]int64 `json:"by_service"`
	ErrorRate   float64          `json:"error_rate"`
	WindowStart time.Time        `json:"window_start"`
	WindowEnd   time.Time        `json:"window_end"`
}

// AggregateStats runs a multi-dimensional aggregation using $facet.
func (s *Store) AggregateStats(ctx context.Context, since time.Time) (*Stats, error) {
	now := time.Now().UTC()
	pipeline := bson.A{
		bson.M{"$match": bson.M{"timestamp": bson.M{"$gte": since}}},
		bson.M{"$facet": bson.M{
			"total":     bson.A{bson.M{"$count": "n"}},
			"by_level":  bson.A{bson.M{"$group": bson.M{"_id": "$level", "n": bson.M{"$sum": 1}}}},
			"by_service": bson.A{
				bson.M{"$group": bson.M{"_id": "$service_name", "n": bson.M{"$sum": 1}}},
				bson.M{"$sort": bson.M{"n": -1}},
				bson.M{"$limit": 20},
			},
		}},
	}

	cur, err := s.logs.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var raw []bson.M
	if err := cur.All(ctx, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return &Stats{ByLevel: map[string]int64{}, ByService: map[string]int64{}, WindowStart: since, WindowEnd: now}, nil
	}

	stats := &Stats{
		ByLevel:     map[string]int64{},
		ByService:   map[string]int64{},
		WindowStart: since,
		WindowEnd:   now,
	}

	if totals, ok := raw[0]["total"].(bson.A); ok && len(totals) > 0 {
		if doc, ok := totals[0].(bson.M); ok {
			stats.TotalEvents = toInt64(doc["n"])
		}
	}
	if levels, ok := raw[0]["by_level"].(bson.A); ok {
		var errCount int64
		for _, item := range levels {
			if doc, ok := item.(bson.M); ok {
				lvl, _ := doc["_id"].(string)
				n := toInt64(doc["n"])
				stats.ByLevel[lvl] = n
				if lvl == string(models.LevelError) || lvl == string(models.LevelFatal) {
					errCount += n
				}
			}
		}
		if stats.TotalEvents > 0 {
			stats.ErrorRate = float64(errCount) / float64(stats.TotalEvents)
		}
	}
	if services, ok := raw[0]["by_service"].(bson.A); ok {
		for _, item := range services {
			if doc, ok := item.(bson.M); ok {
				svc, _ := doc["_id"].(string)
				stats.ByService[svc] = toInt64(doc["n"])
			}
		}
	}
	return stats, nil
}

// BucketByMinute returns minute-level event counts in the time range.
// Used by the demo dashboard for the throughput chart.
func (s *Store) BucketByMinute(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	pipeline := bson.A{
		bson.M{"$match": bson.M{"timestamp": bson.M{"$gte": since}}},
		bson.M{"$group": bson.M{
			"_id": bson.M{
				"$dateTrunc": bson.M{"date": "$timestamp", "unit": "minute"},
			},
			"count":  bson.M{"$sum": 1},
			"errors": bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$in": bson.A{"$level", bson.A{"ERROR", "FATAL"}}}, 1, 0}}},
		}},
		bson.M{"$sort": bson.M{"_id": 1}},
		bson.M{"$limit": 120},
	}

	cur, err := s.logs.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var raw []bson.M
	if err := cur.All(ctx, &raw); err != nil {
		return nil, err
	}

	out := make([]map[string]interface{}, 0, len(raw))
	for _, r := range raw {
		ts, _ := r["_id"].(time.Time)
		out = append(out, map[string]interface{}{
			"ts":     ts.Format(time.RFC3339),
			"count":  toInt64(r["count"]),
			"errors": toInt64(r["errors"]),
		})
	}
	return out, nil
}

// WatchErrors returns a channel of error/fatal log events using change streams.
// Used by the alerter for <200ms notification latency.
func (s *Store) WatchErrors(ctx context.Context) (<-chan models.LogEvent, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"operationType":           "insert",
			"fullDocument.level":      bson.M{"$in": bson.A{"ERROR", "FATAL"}},
		}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	stream, err := s.logs.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, err
	}

	out := make(chan models.LogEvent, 256)
	go func() {
		defer close(out)
		defer stream.Close(context.Background())
		for stream.Next(ctx) {
			var doc struct {
				FullDocument models.LogEvent `bson:"fullDocument"`
			}
			if err := stream.Decode(&doc); err != nil {
				s.logger.Warn("decode change stream event", zap.Error(err))
				continue
			}
			select {
			case out <- doc.FullDocument:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// LoadRules returns all enabled alert rules.
func (s *Store) LoadRules(ctx context.Context) ([]models.AlertRule, error) {
	cur, err := s.rules.Find(ctx, bson.M{"enabled": true})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []models.AlertRule
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Close gracefully disconnects the MongoDB client.
func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// Client exposes the underlying client (used in integration tests).
func (s *Store) Client() *mongo.Client { return s.client }

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}
