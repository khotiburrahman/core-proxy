package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	defaultLogger *slog.Logger
	once          sync.Once
)

type Config struct {
	Level  string
	Format string
}

func Init(cfg Config) {
	once.Do(func() {
		var level slog.Level
		switch strings.ToUpper(cfg.Level) {
		case "DEBUG":
			level = slog.LevelDebug
		case "INFO":
			level = slog.LevelInfo
		case "WARN":
			level = slog.LevelWarn
		case "ERROR":
			level = slog.LevelError
		default:
			level = slog.LevelInfo
		}

		opts := &slog.HandlerOptions{Level: level}

		var handler slog.Handler
		var writer io.Writer = os.Stdout

		if strings.ToLower(cfg.Format) == "json" {
			handler = slog.NewJSONHandler(writer, opts)
		} else {
			handler = slog.NewTextHandler(writer, opts)
		}

		defaultLogger = slog.New(handler)
		slog.SetDefault(defaultLogger)
	})
}

func Logger() *slog.Logger {
	if defaultLogger == nil {
		Init(Config{Level: "INFO", Format: "text"})
	}
	return defaultLogger
}

func Debug(msg string, args ...any) { Logger().Debug(msg, args...) }
func Info(msg string, args ...any)  { Logger().Info(msg, args...) }
func Warn(msg string, args ...any)  { Logger().Warn(msg, args...) }
func Error(msg string, args ...any) { Logger().Error(msg, args...) }

func Fatal(msg string, args ...any) {
	Logger().Error(msg, args...)
	os.Exit(1)
}

func InitLogger(level string) {
	Init(Config{Level: level, Format: "text"})
}

func With(args ...any) *slog.Logger {
	return Logger().With(args...)
}

func LogCtx(ctx context.Context, level slog.Level, msg string, args ...any) {
	Logger().Log(ctx, level, msg, args...)
}
