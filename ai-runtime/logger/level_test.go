package logger

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want slog.Level
	}{
		{"debug", "debug", slog.LevelDebug},
		{"info", "info", slog.LevelInfo},
		{"warn", "warn", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		{"uppercase", "ERROR", slog.LevelError},
		{"mixed case", "DeBuG", slog.LevelDebug},
		{"trimmed", "  warn  ", slog.LevelWarn},
		{"empty falls back", "", slog.LevelInfo},
		{"unknown falls back", "verbose", slog.LevelInfo},
		{"garbage falls back", "trace", slog.LevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseLevel(tt.in); got != tt.want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidLevel(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"debug", true},
		{"info", true},
		{"warn", true},
		{"error", true},
		{"WARN", true},
		{"", false},
		{"verbose", false},
	}
	for _, tt := range tests {
		if got := ValidLevel(tt.in); got != tt.want {
			t.Errorf("ValidLevel(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestLevelName(t *testing.T) {
	tests := []struct {
		in   slog.Level
		want string
	}{
		{slog.LevelDebug, "debug"},
		{slog.LevelInfo, "info"},
		{slog.LevelWarn, "warn"},
		{slog.LevelError, "error"},
	}
	for _, tt := range tests {
		if got := LevelName(tt.in); got != tt.want {
			t.Errorf("LevelName(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
