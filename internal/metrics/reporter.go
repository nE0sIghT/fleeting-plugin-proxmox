package metrics

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

const (
	DefaultReporterInterval = 15 * time.Second
	defaultReporterRetry    = time.Second
	defaultReporterTimeout  = 5 * time.Second
	defaultMaxProblems      = 128
	defaultProblemMessage   = 2048
)

type ReporterConfig struct {
	SocketPath string
	Interval   time.Duration
	Identity   Identity
	Collect    func(context.Context) (Snapshot, error)
	Info       func(string, ...any)
	Warn       func(string, ...any)
}

type Reporter struct {
	cfg ReporterConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu          sync.Mutex
	lastSendSet bool
	lastSendOK  bool

	problemMu            sync.Mutex
	problemSource        string
	problemSequence      uint64
	pendingProblems      map[string]ProblemEvent
	collectionProblemSet bool
}

func NewReporter(cfg ReporterConfig) *Reporter {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultReporterInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Reporter{
		cfg:             cfg,
		ctx:             ctx,
		cancel:          cancel,
		problemSource:   newProblemSource(),
		pendingProblems: map[string]ProblemEvent{},
	}
}

func (r *Reporter) Start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.run()
	}()
}

func (r *Reporter) Shutdown(ctx context.Context) {
	r.cancel()
	r.wg.Wait()

	if r.cfg.SocketPath == "" {
		return
	}
	r.flushProblems(ctx, Snapshot{
		Version:           1,
		Identity:          r.cfg.Identity,
		TimestampUnix:     time.Now().Unix(),
		Up:                false,
		LastScrapeSuccess: false,
	})
}

func (r *Reporter) run() {
	for {
		err := r.publish(r.ctx)
		r.recordSendResult(err)

		wait := r.cfg.Interval
		if err != nil && wait > defaultReporterRetry {
			wait = defaultReporterRetry
		}

		timer := time.NewTimer(wait)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Reporter) publish(ctx context.Context) error {
	if r.cfg.SocketPath == "" {
		return nil
	}

	collectCtx, cancel := context.WithTimeout(ctx, r.cfg.Interval)
	defer cancel()

	conn, err := r.dial(collectCtx)
	if err != nil {
		return err
	}
	defer conn.Close()

	snapshot, err := r.cfg.Collect(collectCtx)
	if err != nil {
		r.setCollectionProblem(err)
		snapshot = Snapshot{
			Version:           1,
			Identity:          r.cfg.Identity,
			TimestampUnix:     time.Now().Unix(),
			Up:                true,
			LastScrapeSuccess: false,
			LastError:         err.Error(),
		}
	} else {
		r.clearCollectionProblem()
		snapshot.Version = 1
		snapshot.Identity = r.cfg.Identity
		snapshot.TimestampUnix = time.Now().Unix()
		snapshot.Up = true
		snapshot.LastScrapeSuccess = true
	}

	return r.encodeSnapshot(conn, snapshot)
}

func (r *Reporter) flushProblems(ctx context.Context, snapshot Snapshot) {
	sendCtx, cancel := context.WithTimeout(ctx, defaultReporterTimeout)
	defer cancel()

	conn, err := r.dial(sendCtx)
	if err != nil {
		return
	}
	defer conn.Close()

	_ = r.encodeSnapshot(conn, snapshot)
}

func (r *Reporter) FlushProblems(ctx context.Context, up bool) {
	r.flushProblems(ctx, Snapshot{
		Version:           1,
		Identity:          r.cfg.Identity,
		TimestampUnix:     time.Now().Unix(),
		Up:                up,
		LastScrapeSuccess: up,
	})
}

func (r *Reporter) ReportProblem(event ProblemEvent) {
	if event.Code == "" {
		return
	}
	if event.State == "" {
		event.State = ProblemRecent
	}
	if event.Severity == "" {
		event.Severity = "error"
	}
	if event.TimestampUnix == 0 {
		event.TimestampUnix = time.Now().Unix()
	}
	if event.State != ProblemResolved && event.Occurrences == 0 {
		event.Occurrences = 1
	}
	if len(event.Message) > defaultProblemMessage {
		event.Message = event.Message[:defaultProblemMessage]
	}

	r.problemMu.Lock()
	defer r.problemMu.Unlock()
	r.mergeProblemLocked(event)
}

func (r *Reporter) mergeProblemLocked(event ProblemEvent) {
	key := event.Key()
	if _, exists := r.pendingProblems[key]; !exists && len(r.pendingProblems) >= defaultMaxProblems {
		return
	}
	if pending, ok := r.pendingProblems[key]; ok {
		if event.State != ProblemResolved {
			event.Occurrences += pending.Occurrences
		} else {
			event.Occurrences = pending.Occurrences
			event.Severity = pending.Severity
			event.Instance = pending.Instance
			event.OperationID = pending.OperationID
			event.Message = pending.Message
			event.TimestampUnix = pending.TimestampUnix
		}
	}
	r.problemSequence++
	event.ID = fmt.Sprintf("%s-%x", r.problemSource, r.problemSequence)
	r.pendingProblems[key] = event
}

func (r *Reporter) encodeSnapshot(conn net.Conn, snapshot Snapshot) error {
	r.problemMu.Lock()
	defer r.problemMu.Unlock()

	keys := make([]string, 0, len(r.pendingProblems))
	for key := range r.pendingProblems {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		snapshot.ProblemEvents = append(snapshot.ProblemEvents, r.pendingProblems[key])
	}

	if err := json.NewEncoder(conn).Encode(snapshot); err != nil {
		return err
	}
	for _, key := range keys {
		delete(r.pendingProblems, key)
	}
	return nil
}

func (r *Reporter) setCollectionProblem(err error) {
	r.problemMu.Lock()
	defer r.problemMu.Unlock()

	r.collectionProblemSet = true
	r.mergeProblemLocked(ProblemEvent{
		Code:        "metrics_collection_failed",
		State:       ProblemActive,
		Severity:    "error",
		Phase:       "metrics",
		Message:     err.Error(),
		Occurrences: 1,
	})
}

func (r *Reporter) clearCollectionProblem() {
	r.problemMu.Lock()
	defer r.problemMu.Unlock()

	if !r.collectionProblemSet {
		return
	}
	r.collectionProblemSet = false
	r.mergeProblemLocked(ProblemEvent{
		Code:  "metrics_collection_failed",
		State: ProblemResolved,
		Phase: "metrics",
	})
}

func (r *Reporter) dial(ctx context.Context) (net.Conn, error) {
	dialer := net.Dialer{Timeout: defaultReporterTimeout}
	return dialer.DialContext(ctx, "unix", r.cfg.SocketPath)
}

func (r *Reporter) recordSendResult(err error) {
	ok := err == nil

	r.mu.Lock()
	wasSet := r.lastSendSet
	changed := wasSet && r.lastSendOK != ok
	firstFailure := !wasSet && !ok
	r.lastSendSet = true
	r.lastSendOK = ok
	r.mu.Unlock()

	if !changed && !firstFailure {
		return
	}
	if ok {
		if r.cfg.Info != nil {
			r.cfg.Info("metrics exporter connection restored", "socket", r.cfg.SocketPath)
		}
		return
	}
	if r.cfg.Warn != nil {
		r.cfg.Warn("metrics exporter unavailable", "socket", r.cfg.SocketPath, "error", err)
	}
}

func newProblemSource() string {
	var data [8]byte
	if _, err := rand.Read(data[:]); err == nil {
		return fmt.Sprintf("%x", data[:])
	}
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
