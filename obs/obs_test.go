package obs_test

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/Ocean-Gaming/platform-go/obs"
)

func TestParseLevelReadsWhatOperatorsActuallyWrite(t *testing.T) {
	for _, c := range []struct {
		raw  string
		want slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"  Debug  ", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
	} {
		got, err := obs.ParseLevel(c.raw)
		if err != nil {
			t.Errorf("ParseLevel(%q) errored: %v", c.raw, err)
		}
		if got != c.want {
			t.Errorf("ParseLevel(%q) = %s, want %s", c.raw, got, c.want)
		}
	}
}

// A level nobody recognises must fall back to info AND report why.
//
// Returning info with a nil error is the whole bug this package now exists to
// prevent, one layer further in: the operator asks for debug, gets info, and
// spends the incident wondering why the logs are thin.
func TestAnUnknownLevelIsReportedRatherThanSwallowed(t *testing.T) {
	got, err := obs.ParseLevel("debg")
	if err == nil {
		t.Fatal("no error; a mistyped level must be reported, not silently downgraded")
	}
	if !strings.Contains(err.Error(), "debg") {
		t.Errorf("error does not name the offending value: %v", err)
	}
	if got != slog.LevelInfo {
		t.Errorf("level = %s, want info as the safe fallback", got)
	}
}

// NewLoggerFromEnv must actually apply the level, not merely read it.
//
// The regression being pinned is not hypothetical: every caller of NewLogger
// on the platform passed slog.LevelInfo, so LOG_LEVEL was set everywhere and
// honoured almost nowhere.
func TestNewLoggerFromEnvAppliesTheLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	if !obs.NewLoggerFromEnv("svc").Enabled(t.Context(), slog.LevelDebug) {
		t.Fatal("LOG_LEVEL=debug did not enable debug logging")
	}

	t.Setenv("LOG_LEVEL", "error")
	log := obs.NewLoggerFromEnv("svc")
	if log.Enabled(t.Context(), slog.LevelWarn) {
		t.Error("LOG_LEVEL=error still logs warnings")
	}
	if !log.Enabled(t.Context(), slog.LevelError) {
		t.Error("LOG_LEVEL=error dropped errors")
	}
}
