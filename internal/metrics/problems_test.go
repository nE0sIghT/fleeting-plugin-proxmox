package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProblemAggregationLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	exporter := NewExporter(ExporterConfig{
		ExceptionsFile:   filepath.Join(t.TempDir(), "exceptions"),
		StaleAfter:       2 * time.Hour,
		ProblemRetention: time.Hour,
		ProblemLimit:     100,
	})
	identity := Identity{Cluster: "prod", Pool: "ci", Group: "arm64"}
	exporter.snapshots[identity.Key()] = storedSnapshot{
		Snapshot:     Snapshot{Identity: identity, Up: true},
		ReceivedUnix: now.Unix(),
	}

	exporter.applyProblemEventsLocked(identity, []ProblemEvent{
		{
			ID:              "event-1",
			Code:            "clone_volume_collision",
			State:           ProblemActive,
			Severity:        "error",
			Phase:           "clone",
			Node:            "pve01",
			Storage:         "nvme0",
			Instance:        "pve01/5000",
			Message:         "disk already exists",
			TimestampUnix:   now.Unix(),
			ActiveUntilUnix: now.Add(30 * time.Minute).Unix(),
			Occurrences:     2,
		},
		{
			ID:              "event-1",
			Code:            "clone_volume_collision",
			State:           ProblemActive,
			Phase:           "clone",
			Node:            "pve01",
			Storage:         "nvme0",
			TimestampUnix:   now.Unix(),
			ActiveUntilUnix: now.Add(30 * time.Minute).Unix(),
			Occurrences:     2,
		},
	}, now)

	status := exporter.problemStatus(now)
	require.Equal(t, "ERROR", status.Status)
	require.Equal(t, 1, status.Active)
	require.Zero(t, status.Recent)
	require.Len(t, status.Problems, 1)
	require.Equal(t, uint64(2), status.Problems[0].Occurrences)

	status = exporter.problemStatus(now.Add(31 * time.Minute))
	require.Equal(t, "DEGRADED", status.Status)
	require.Zero(t, status.Active)
	require.Equal(t, 1, status.Recent)
	require.Equal(t, ProblemRecent, status.Problems[0].State)

	status = exporter.problemStatus(now.Add(61 * time.Minute))
	require.Equal(t, "OK", status.Status)
	require.Empty(t, status.Problems)
}

func TestResolvedProblemRemainsRecentUntilRetention(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	exporter := NewExporter(ExporterConfig{
		ExceptionsFile:   filepath.Join(t.TempDir(), "exceptions"),
		StaleAfter:       time.Hour,
		ProblemRetention: 24 * time.Hour,
	})
	identity := Identity{Cluster: "prod", Pool: "ci", Group: "amd64"}
	exporter.snapshots[identity.Key()] = storedSnapshot{
		Snapshot:     Snapshot{Identity: identity, Up: true},
		ReceivedUnix: now.Unix(),
	}
	exporter.applyProblemEventsLocked(identity, []ProblemEvent{{
		ID:            "failed",
		Code:          "init_failed",
		State:         ProblemActive,
		Phase:         "init",
		Message:       "API unavailable",
		TimestampUnix: now.Unix(),
	}}, now)
	exporter.applyProblemEventsLocked(identity, []ProblemEvent{{
		ID:            "resolved",
		Code:          "init_failed",
		State:         ProblemResolved,
		Phase:         "init",
		TimestampUnix: now.Add(time.Minute).Unix(),
	}}, now.Add(time.Minute))

	status := exporter.problemStatus(now.Add(time.Minute))
	require.Equal(t, "DEGRADED", status.Status)
	require.Zero(t, status.Active)
	require.Equal(t, 1, status.Recent)
	require.Equal(t, "API unavailable", status.Problems[0].Message)
}

func TestExceptionsFileIsHumanReadableAndAtomic(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "runtime", "exceptions")
	exporter := NewExporter(ExporterConfig{
		ExceptionsFile:   path,
		ProblemRetention: time.Hour,
		StaleAfter:       time.Minute,
	})
	identity := Identity{Cluster: "prod", Pool: "ci", Group: "arm64"}
	exporter.snapshots[identity.Key()] = storedSnapshot{
		Snapshot:     Snapshot{Identity: identity, Up: true},
		ReceivedUnix: now.Unix(),
	}
	exporter.applyProblemEventsLocked(identity, []ProblemEvent{{
		ID:            "event-1",
		Code:          "start_failed",
		State:         ProblemRecent,
		Phase:         "start",
		Node:          "pve01",
		Instance:      "pve01/5000",
		OperationID:   "inc-1-1",
		Message:       "guest agent\nnot ready",
		TimestampUnix: now.Unix(),
	}}, now)

	require.NoError(t, exporter.refreshExceptionsFile(now))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "STATUS: DEGRADED")
	require.Contains(t, string(data), "prod  ci  arm64  start_failed")
	require.Contains(t, string(data), `last_error="guest agent not ready"`)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".exceptions-*"))
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestProblemStatusUnknownWithoutFleetSnapshots(t *testing.T) {
	t.Parallel()

	exporter := NewExporter(ExporterConfig{ExceptionsFile: filepath.Join(t.TempDir(), "exceptions")})
	status := exporter.problemStatus(time.Now())

	require.Equal(t, "UNKNOWN", status.Status)
	require.Zero(t, status.Fleets)
}

func TestProblemHTTPHandlers(t *testing.T) {
	t.Parallel()

	now := time.Now()
	exporter := NewExporter(ExporterConfig{
		ExceptionsFile:   filepath.Join(t.TempDir(), "exceptions"),
		ProblemRetention: time.Hour,
		StaleAfter:       time.Hour,
	})
	identity := Identity{Cluster: "prod", Pool: "ci", Group: "arm64"}
	exporter.snapshots[identity.Key()] = storedSnapshot{
		Snapshot:     Snapshot{Identity: identity, Up: true},
		ReceivedUnix: now.Unix(),
	}

	textResponse := httptest.NewRecorder()
	exporter.handleProblems(textResponse, httptest.NewRequest("GET", "/problems", nil))
	require.Equal(t, 200, textResponse.Code)
	require.Contains(t, textResponse.Body.String(), "STATUS: OK")

	jsonResponse := httptest.NewRecorder()
	exporter.handleProblemsJSON(jsonResponse, httptest.NewRequest("GET", "/problems.json", nil))
	require.Equal(t, 200, jsonResponse.Code)
	var status ProblemStatus
	require.NoError(t, json.Unmarshal(jsonResponse.Body.Bytes(), &status))
	require.Equal(t, "OK", status.Status)
	require.Equal(t, 1, status.Fleets)
}
