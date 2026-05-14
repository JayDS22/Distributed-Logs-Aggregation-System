package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New constructs a production-grade zap logger with the requested level and
// format. Format is one of: "json" or "console".
func New(level, format string) (*zap.Logger, error) {
	lvl := zap.NewAtomicLevel()
	switch strings.ToLower(level) {
	case "debug":
		lvl.SetLevel(zap.DebugLevel)
	case "warn":
		lvl.SetLevel(zap.WarnLevel)
	case "error":
		lvl.SetLevel(zap.ErrorLevel)
	default:
		lvl.SetLevel(zap.InfoLevel)
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var enc zapcore.Encoder
	if format == "console" {
		enc = zapcore.NewConsoleEncoder(encCfg)
	} else {
		enc = zapcore.NewJSONEncoder(encCfg)
	}

	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), lvl)
	return zap.New(core, zap.AddCaller()), nil
}
