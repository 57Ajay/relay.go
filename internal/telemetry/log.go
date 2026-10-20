// Package telemetry holds logging, health checks, metrics and tracing setup.
package telemetry

import (
	"io"
	"log/slog"
)

func NewLogger(w io.Writer, service, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})).With("service", service)
}
