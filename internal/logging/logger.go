package logging

import (
	"io"
	"log"
	"strings"
)

// Level represents the logger verbosity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// Logger emits log lines based on a configured level.
type Logger struct {
	level  Level
	logger *log.Logger
}

// New constructs a logger writing to the provided output.
func New(output io.Writer, level string) *Logger {
	return &Logger{
		level:  parseLevel(level),
		logger: log.New(output, "", log.LstdFlags),
	}
}

// Debugf logs a debug message.
func (l *Logger) Debugf(format string, args ...any) {
	l.logf(LevelDebug, "debug: ", format, args...)
}

// Infof logs an info message.
func (l *Logger) Infof(format string, args ...any) {
	l.logf(LevelInfo, "info: ", format, args...)
}

// Warnf logs a warning message.
func (l *Logger) Warnf(format string, args ...any) {
	l.logf(LevelWarn, "warn: ", format, args...)
}

// Errorf logs an error message.
func (l *Logger) Errorf(format string, args ...any) {
	l.logf(LevelError, "error: ", format, args...)
}

func (l *Logger) logf(level Level, prefix, format string, args ...any) {
	if l.level > level {
		return
	}
	l.logger.Printf(prefix+format, args...)
}

func parseLevel(value string) Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}
