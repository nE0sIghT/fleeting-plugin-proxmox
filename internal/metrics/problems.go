package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type storedProblem struct {
	Identity        Identity
	Code            string
	Severity        string
	Phase           string
	Node            string
	Storage         string
	Instance        string
	OperationID     string
	Message         string
	Occurrences     uint64
	FirstSeenUnix   int64
	LastSeenUnix    int64
	Active          bool
	ActiveUntilUnix int64
}

type ProblemStatus struct {
	Status      string        `json:"status"`
	GeneratedAt string        `json:"generated_at"`
	Fleets      int           `json:"fleets"`
	Active      int           `json:"active"`
	Recent      int           `json:"recent"`
	Problems    []ProblemView `json:"problems,omitempty"`
}

type ProblemView struct {
	State           ProblemState `json:"state"`
	Cluster         string       `json:"cluster"`
	Pool            string       `json:"pool"`
	Group           string       `json:"group"`
	Code            string       `json:"code"`
	Severity        string       `json:"severity"`
	Phase           string       `json:"phase,omitempty"`
	Node            string       `json:"node,omitempty"`
	Storage         string       `json:"storage,omitempty"`
	Instance        string       `json:"instance,omitempty"`
	OperationID     string       `json:"operation_id,omitempty"`
	Message         string       `json:"message,omitempty"`
	Occurrences     uint64       `json:"occurrences"`
	FirstSeenUnix   int64        `json:"first_seen_unix"`
	LastSeenUnix    int64        `json:"last_seen_unix"`
	ActiveUntilUnix int64        `json:"active_until_unix,omitempty"`
}

func problemMapKey(identity Identity, event ProblemEvent) string {
	return identity.Key() + "\x00" + event.Key()
}

func (e *Exporter) applyProblemEventsLocked(identity Identity, events []ProblemEvent, receivedAt time.Time) {
	for _, event := range events {
		if event.Code == "" {
			continue
		}
		if event.ID != "" {
			if _, seen := e.seenProblemEvents[event.ID]; seen {
				continue
			}
			e.seenProblemEvents[event.ID] = receivedAt.Unix()
		}

		key := problemMapKey(identity, event)
		problem, exists := e.problems[key]
		if event.State == ProblemResolved {
			if event.Occurrences > 0 {
				problem = applyProblemOccurrence(problem, identity, event, receivedAt)
				exists = true
			}
			if exists {
				problem.Active = false
				problem.ActiveUntilUnix = 0
				e.problems[key] = problem
			}
			continue
		}

		problem = applyProblemOccurrence(problem, identity, event, receivedAt)
		problem.Active = event.State == ProblemActive
		e.problems[key] = problem
	}
}

func applyProblemOccurrence(problem storedProblem, identity Identity, event ProblemEvent, receivedAt time.Time) storedProblem {
	occurredAt := event.TimestampUnix
	if occurredAt == 0 {
		occurredAt = receivedAt.Unix()
	}
	occurrences := event.Occurrences
	if occurrences == 0 {
		occurrences = 1
	}
	if problem.Code == "" {
		problem.Identity = identity
		problem.Code = event.Code
		problem.Phase = event.Phase
		problem.Node = event.Node
		problem.Storage = event.Storage
		problem.FirstSeenUnix = occurredAt
	}
	problem.Occurrences += occurrences
	if occurredAt >= problem.LastSeenUnix {
		problem.Severity = event.Severity
		if problem.Severity == "" {
			problem.Severity = "error"
		}
		problem.Instance = event.Instance
		problem.OperationID = event.OperationID
		problem.Message = event.Message
		problem.LastSeenUnix = occurredAt
		problem.ActiveUntilUnix = event.ActiveUntilUnix
	}
	return problem
}

func (e *Exporter) problemStatus(now time.Time) ProblemStatus {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pruneProblemsLocked(now)
	status := ProblemStatus{
		Status:      "UNKNOWN",
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Fleets:      len(e.snapshots),
	}
	activeIdentities := map[string]bool{}

	for _, problem := range e.problems {
		view := problemView(problem)
		if problem.Active {
			view.State = ProblemActive
			status.Active++
			activeIdentities[problem.Identity.Key()] = true
		} else {
			view.State = ProblemRecent
			status.Recent++
		}
		status.Problems = append(status.Problems, view)
	}

	for _, snapshot := range e.snapshots {
		if !snapshotStale(snapshot, now, e.cfg.StaleAfter) {
			continue
		}
		if !snapshot.Snapshot.Up && activeIdentities[snapshot.Snapshot.Identity.Key()] {
			continue
		}
		code := "reporter_stale"
		message := "plugin metrics reporter has not updated within the stale interval"
		if !snapshot.Snapshot.Up {
			code = "reporter_offline"
			message = "plugin metrics reporter stopped"
		}
		status.Active++
		status.Problems = append(status.Problems, ProblemView{
			State:         ProblemActive,
			Cluster:       snapshot.Snapshot.Identity.Cluster,
			Pool:          snapshot.Snapshot.Identity.Pool,
			Group:         snapshot.Snapshot.Identity.Group,
			Code:          code,
			Severity:      "error",
			Phase:         "metrics",
			Message:       message,
			Occurrences:   1,
			FirstSeenUnix: snapshot.ReceivedUnix,
			LastSeenUnix:  snapshot.ReceivedUnix,
		})
	}

	sort.Slice(status.Problems, func(i, j int) bool {
		if status.Problems[i].State != status.Problems[j].State {
			return status.Problems[i].State == ProblemActive
		}
		if status.Problems[i].LastSeenUnix != status.Problems[j].LastSeenUnix {
			return status.Problems[i].LastSeenUnix > status.Problems[j].LastSeenUnix
		}
		if status.Problems[i].Occurrences != status.Problems[j].Occurrences {
			return status.Problems[i].Occurrences > status.Problems[j].Occurrences
		}
		return problemViewKey(status.Problems[i]) < problemViewKey(status.Problems[j])
	})
	switch {
	case status.Active > 0:
		status.Status = "ERROR"
	case status.Recent > 0:
		status.Status = "DEGRADED"
	case status.Fleets > 0:
		status.Status = "OK"
	}
	return status
}

func limitProblemStatus(status ProblemStatus, limit int) ProblemStatus {
	if limit > 0 && len(status.Problems) > limit {
		status.Problems = status.Problems[:limit]
	}
	return status
}

func (e *Exporter) pruneProblemsLocked(now time.Time) {
	for key, problem := range e.problems {
		if problem.Active && problem.ActiveUntilUnix > 0 && now.Unix() >= problem.ActiveUntilUnix {
			problem.Active = false
			problem.ActiveUntilUnix = 0
			e.problems[key] = problem
		}
		if !problem.Active && e.cfg.ProblemRetention > 0 && now.Sub(time.Unix(problem.LastSeenUnix, 0)) > e.cfg.ProblemRetention {
			delete(e.problems, key)
		}
	}
	for id, seenAt := range e.seenProblemEvents {
		if e.cfg.ProblemRetention > 0 && now.Sub(time.Unix(seenAt, 0)) > e.cfg.ProblemRetention {
			delete(e.seenProblemEvents, id)
		}
	}
}

func snapshotStale(snapshot storedSnapshot, now time.Time, staleAfter time.Duration) bool {
	if !snapshot.Snapshot.Up {
		return true
	}
	if staleAfter <= 0 {
		return false
	}
	return now.Sub(time.Unix(snapshot.ReceivedUnix, 0)) > staleAfter
}

func problemView(problem storedProblem) ProblemView {
	return ProblemView{
		Cluster:         problem.Identity.Cluster,
		Pool:            problem.Identity.Pool,
		Group:           problem.Identity.Group,
		Code:            problem.Code,
		Severity:        problem.Severity,
		Phase:           problem.Phase,
		Node:            problem.Node,
		Storage:         problem.Storage,
		Instance:        problem.Instance,
		OperationID:     problem.OperationID,
		Message:         problem.Message,
		Occurrences:     problem.Occurrences,
		FirstSeenUnix:   problem.FirstSeenUnix,
		LastSeenUnix:    problem.LastSeenUnix,
		ActiveUntilUnix: problem.ActiveUntilUnix,
	}
}

func problemViewKey(problem ProblemView) string {
	return strings.Join([]string{problem.Cluster, problem.Pool, problem.Group, problem.Code, problem.Phase, problem.Node, problem.Storage}, "\x00")
}

func renderProblemText(status ProblemStatus) []byte {
	var out bytes.Buffer
	fmt.Fprintf(&out, "STATUS: %s\n", status.Status)
	fmt.Fprintf(&out, "updated: %s\n", status.GeneratedAt)
	fmt.Fprintf(&out, "fleets: %d, active: %d, recent: %d\n", status.Fleets, status.Active, status.Recent)

	currentState := ProblemState("")
	for _, problem := range status.Problems {
		if problem.State != currentState {
			currentState = problem.State
			fmt.Fprintf(&out, "\n%s\n", strings.ToUpper(string(currentState)))
		}
		fmt.Fprintf(
			&out,
			"%s  %s  %s  %s\n",
			problem.Cluster,
			problem.Pool,
			problem.Group,
			problem.Code,
		)
		fmt.Fprintf(
			&out,
			"count=%d  first=%s  last=%s",
			problem.Occurrences,
			formatProblemTime(problem.FirstSeenUnix),
			formatProblemTime(problem.LastSeenUnix),
		)
		if problem.Node != "" {
			fmt.Fprintf(&out, "  node=%s", problem.Node)
		}
		if problem.Storage != "" {
			fmt.Fprintf(&out, "  storage=%s", problem.Storage)
		}
		if problem.Phase != "" {
			fmt.Fprintf(&out, "  phase=%s", problem.Phase)
		}
		fmt.Fprintln(&out)
		if problem.Instance != "" || problem.OperationID != "" {
			var fields []string
			if problem.Instance != "" {
				fields = append(fields, "instance="+problem.Instance)
			}
			if problem.OperationID != "" {
				fields = append(fields, "operation_id="+problem.OperationID)
			}
			fmt.Fprintln(&out, strings.Join(fields, "  "))
		}
		if problem.Message != "" {
			fmt.Fprintf(&out, "last_error=%q\n", singleLine(problem.Message))
		}
	}
	return out.Bytes()
}

func renderProblemJSON(status ProblemStatus) ([]byte, error) {
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (e *Exporter) refreshExceptionsFile(now time.Time) error {
	if e.cfg.ExceptionsFile == "" {
		return nil
	}
	e.fileMu.Lock()
	defer e.fileMu.Unlock()
	status := limitProblemStatus(e.problemStatus(now), e.cfg.ProblemLimit)
	return writeAtomicFile(e.cfg.ExceptionsFile, renderProblemText(status), 0o640)
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create exceptions directory: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".exceptions-*")
	if err != nil {
		return fmt.Errorf("create temporary exceptions file: %w", err)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)

	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return fmt.Errorf("chmod temporary exceptions file: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temporary exceptions file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary exceptions file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace exceptions file: %w", err)
	}
	return nil
}

func formatProblemTime(timestamp int64) string {
	if timestamp == 0 {
		return "-"
	}
	return time.Unix(timestamp, 0).UTC().Format(time.RFC3339)
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
