package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level application configuration.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	MongoDB  MongoDBConfig  `yaml:"mongodb"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Redis    RedisConfig    `yaml:"redis"`
	Ingestor IngestorConfig `yaml:"ingestor"`
	Alerter  AlerterConfig  `yaml:"alerter"`
	Log      LogConfig      `yaml:"log"`
}

type ServerConfig struct {
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	MetricsPort     int           `yaml:"metrics_port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type MongoDBConfig struct {
	URI                string        `yaml:"uri"`
	Database           string        `yaml:"database"`
	Collection         string        `yaml:"collection"`
	RulesCollection    string        `yaml:"rules_collection"`
	ConnectTimeout     time.Duration `yaml:"connect_timeout"`
	MaxPoolSize        uint64        `yaml:"max_pool_size"`
	MinPoolSize        uint64        `yaml:"min_pool_size"`
	TTLDays            int           `yaml:"ttl_days"`
	TimeSeries         bool          `yaml:"time_series"`
	GranularitySeconds bool          `yaml:"granularity_seconds"`
}

type KafkaConfig struct {
	Enabled bool     `yaml:"enabled"`
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupID string   `yaml:"group_id"`
}

type RedisConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	TTLSecs  int    `yaml:"ttl_secs"`
}

type IngestorConfig struct {
	Workers     int `yaml:"workers"`
	BufferSize  int `yaml:"buffer_size"`
	BatchSize   int `yaml:"batch_size"`
	FlushMillis int `yaml:"flush_millis"`
}

type AlerterConfig struct {
	Enabled      bool   `yaml:"enabled"`
	SlackWebhook string `yaml:"slack_webhook"`
	PagerDutyKey string `yaml:"pagerduty_key"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns a Config populated with sensible defaults for local dev.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			MetricsPort:     9090,
			ReadTimeout:     15 * time.Second,
			WriteTimeout:    15 * time.Second,
			ShutdownTimeout: 30 * time.Second,
		},
		MongoDB: MongoDBConfig{
			URI:             "mongodb://localhost:27017",
			Database:        "logstream",
			Collection:      "logs",
			RulesCollection: "alert_rules",
			ConnectTimeout:  10 * time.Second,
			MaxPoolSize:     200,
			MinPoolSize:     10,
			TTLDays:         14,
			TimeSeries:      true,
		},
		Kafka: KafkaConfig{
			Enabled: false,
			Brokers: []string{"localhost:9092"},
			Topic:   "logs",
			GroupID: "logstream-ingestor",
		},
		Redis: RedisConfig{
			Enabled: false,
			Addr:    "localhost:6379",
			DB:      0,
			TTLSecs: 300,
		},
		Ingestor: IngestorConfig{
			Workers:     16,
			BufferSize:  20000,
			BatchSize:   100,
			FlushMillis: 250,
		},
		Alerter: AlerterConfig{
			Enabled: false,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// Load reads configuration from the given YAML path. Environment variables
// override file values where present so the same config can be reused across
// Docker, Kubernetes, and local environments.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("MONGODB_URI"); v != "" {
		cfg.MongoDB.URI = v
	}
	if v := os.Getenv("MONGODB_DATABASE"); v != "" {
		cfg.MongoDB.Database = v
	}
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
		cfg.Redis.Enabled = true
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = []string{v}
		cfg.Kafka.Enabled = true
	}
	if v := os.Getenv("SLACK_WEBHOOK"); v != "" {
		cfg.Alerter.SlackWebhook = v
		cfg.Alerter.Enabled = true
	}
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = p
		}
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Log.Level = v
	}
}
