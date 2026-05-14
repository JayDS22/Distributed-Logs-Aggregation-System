package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JayDS22/logstream/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefault(t *testing.T) {
	c := config.Default()
	assert.Equal(t, 8080, c.Server.Port)
	assert.Equal(t, "logstream", c.MongoDB.Database)
	assert.Equal(t, 16, c.Ingestor.Workers)
	assert.True(t, c.MongoDB.TimeSeries)
}

func TestLoad_MissingFile_ReturnsDefaults(t *testing.T) {
	c, err := config.Load("/nonexistent/path.yaml")
	require.NoError(t, err)
	assert.Equal(t, 8080, c.Server.Port)
}

func TestLoad_YAMLOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
server:
  port: 9000
ingestor:
  workers: 64
  batch_size: 250
mongodb:
  database: mydb
`
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	c, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, 9000, c.Server.Port)
	assert.Equal(t, 64, c.Ingestor.Workers)
	assert.Equal(t, 250, c.Ingestor.BatchSize)
	assert.Equal(t, "mydb", c.MongoDB.Database)
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Setenv("MONGODB_URI", "mongodb://envhost:27017")
	t.Setenv("SERVER_PORT", "12345")
	t.Setenv("LOG_LEVEL", "debug")

	c, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "mongodb://envhost:27017", c.MongoDB.URI)
	assert.Equal(t, 12345, c.Server.Port)
	assert.Equal(t, "debug", c.Log.Level)
}
