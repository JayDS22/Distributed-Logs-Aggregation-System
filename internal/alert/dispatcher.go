package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/JayDS22/logstream/internal/metrics"
	"github.com/JayDS22/logstream/internal/models"
	"github.com/JayDS22/logstream/internal/storage"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

// Dispatcher consumes a change-stream of critical events and dispatches
// notifications to configured channels, protected by a circuit breaker.
type Dispatcher struct {
	cfg        config.AlerterConfig
	store      *storage.Store
	logger     *zap.Logger
	collectors *metrics.Collectors
	client     *http.Client
	breaker    *gobreaker.CircuitBreaker

	mu       sync.RWMutex
	rules    []compiledRule
	rulesAt  time.Time
}

type compiledRule struct {
	rule    models.AlertRule
	pattern *regexp.Regexp
}

// New constructs a Dispatcher.
func New(cfg config.AlerterConfig, store *storage.Store, log *zap.Logger, m *metrics.Collectors) *Dispatcher {
	cbSettings := gobreaker.Settings{
		Name:        "alert-dispatcher",
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     20 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= 5
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Warn("alert circuit breaker state change",
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
		},
	}
	return &Dispatcher{
		cfg:        cfg,
		store:      store,
		logger:     log,
		collectors: m,
		client:     &http.Client{Timeout: 5 * time.Second},
		breaker:    gobreaker.NewCircuitBreaker(cbSettings),
	}
}

// Run starts the change-stream consumer. Returns when ctx is canceled.
func (d *Dispatcher) Run(ctx context.Context) error {
	if err := d.refreshRules(ctx); err != nil {
		d.logger.Warn("initial alert rules load failed", zap.Error(err))
	}

	// Periodically reload rules.
	go d.rulesRefresher(ctx)

	stream, err := d.store.WatchErrors(ctx)
	if err != nil {
		return fmt.Errorf("open change stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-stream:
			if !ok {
				return nil
			}
			d.evaluate(ctx, ev)
		}
	}
}

func (d *Dispatcher) rulesRefresher(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := d.refreshRules(ctx); err != nil {
				d.logger.Warn("rules refresh failed", zap.Error(err))
			}
		}
	}
}

func (d *Dispatcher) refreshRules(ctx context.Context) error {
	rules, err := d.store.LoadRules(ctx)
	if err != nil {
		return err
	}
	compiled := make([]compiledRule, 0, len(rules))
	for _, r := range rules {
		cr := compiledRule{rule: r}
		if r.Pattern != "" {
			re, err := regexp.Compile(r.Pattern)
			if err != nil {
				d.logger.Warn("invalid alert rule pattern", zap.String("rule", r.Name), zap.Error(err))
				continue
			}
			cr.pattern = re
		}
		compiled = append(compiled, cr)
	}
	d.mu.Lock()
	d.rules = compiled
	d.rulesAt = time.Now()
	d.mu.Unlock()
	d.logger.Info("alert rules refreshed", zap.Int("count", len(compiled)))
	return nil
}

func (d *Dispatcher) evaluate(ctx context.Context, ev models.LogEvent) {
	d.mu.RLock()
	rules := d.rules
	d.mu.RUnlock()

	// Default rule: any FATAL event always alerts even without configured rules.
	if ev.Level == models.LevelFatal && len(rules) == 0 {
		d.fire(ctx, models.AlertRule{Name: "fatal-default", Channel: "slack"}, ev)
		return
	}

	for _, cr := range rules {
		if cr.rule.ServiceName != "" && cr.rule.ServiceName != ev.ServiceName {
			continue
		}
		if cr.rule.Level != "" && cr.rule.Level != ev.Level {
			continue
		}
		if cr.pattern != nil && !cr.pattern.MatchString(ev.Message) {
			continue
		}
		d.fire(ctx, cr.rule, ev)
	}
}

func (d *Dispatcher) fire(ctx context.Context, rule models.AlertRule, ev models.LogEvent) {
	if !d.cfg.Enabled {
		// Just record the fact that we'd have fired.
		if d.collectors != nil {
			d.collectors.AlertsFiredTotal.Inc()
		}
		d.logger.Info("alert (disabled)",
			zap.String("rule", rule.Name),
			zap.String("service", ev.ServiceName),
			zap.String("level", string(ev.Level)),
			zap.String("message", ev.Message),
		)
		return
	}

	payload := buildSlackPayload(rule, ev)
	target := d.cfg.SlackWebhook
	if rule.Webhook != "" {
		target = rule.Webhook
	}
	if target == "" {
		d.logger.Warn("no webhook configured for alert", zap.String("rule", rule.Name))
		return
	}

	_, err := d.breaker.Execute(func() (interface{}, error) {
		return nil, d.post(ctx, target, payload)
	})
	if err != nil {
		d.logger.Warn("alert dispatch failed",
			zap.String("rule", rule.Name),
			zap.Error(err),
		)
		return
	}
	if d.collectors != nil {
		d.collectors.AlertsFiredTotal.Inc()
	}
}

func (d *Dispatcher) post(ctx context.Context, url string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("alert webhook returned %d", resp.StatusCode)
	}
	return nil
}

func buildSlackPayload(rule models.AlertRule, ev models.LogEvent) []byte {
	emoji := ":rotating_light:"
	if ev.Level == models.LevelFatal {
		emoji = ":fire:"
	}
	text := fmt.Sprintf("%s *Alert: %s*\n*Service:* `%s` | *Level:* `%s`\n```%s```",
		emoji, rule.Name, ev.ServiceName, ev.Level,
		strings.ReplaceAll(ev.Message, "`", "'"),
	)
	b, _ := json.Marshal(map[string]string{"text": text})
	return b
}
