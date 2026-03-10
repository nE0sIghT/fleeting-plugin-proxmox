package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type LeaseRecord struct {
	Key         string     `json:"key,omitempty"`
	AllocatedAt time.Time  `json:"allocated_at,omitempty"`
	ReleasedAt  *time.Time `json:"released_at,omitempty"`
}

type Snapshot struct {
	Leases map[string]LeaseRecord `json:"leases"`
}

type Store interface {
	Read(context.Context) (Snapshot, error)
	Update(context.Context, func(*Snapshot) error) error
}

type FileStore struct {
	path     string
	lockPath string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{
		path:     path,
		lockPath: path + ".lock",
	}
}

func (s *FileStore) Read(ctx context.Context) (Snapshot, error) {
	var out Snapshot
	err := s.withLock(ctx, func() error {
		snapshot, err := s.load()
		if err != nil {
			return err
		}
		out = snapshot
		return nil
	})
	return out, err
}

func (s *FileStore) Update(ctx context.Context, fn func(*Snapshot) error) error {
	return s.withLock(ctx, func() error {
		snapshot, err := s.load()
		if err != nil {
			return err
		}
		if err := fn(&snapshot); err != nil {
			return err
		}
		return s.save(snapshot)
	})
}

func (s *FileStore) withLock(ctx context.Context, fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(s.lockPath), 0o755); err != nil {
		return fmt.Errorf("create lock dir: %w", err)
	}

	lockFile, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	defer lockFile.Close()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			if errors.Is(err, syscall.EWOULDBLOCK) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return fmt.Errorf("lock state file: %w", err)
		}
		break
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	return fn()
}

func (s *FileStore) load() (Snapshot, error) {
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{Leases: map[string]LeaseRecord{}}, nil
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("open state file: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read state file: %w", err)
	}
	if len(content) == 0 {
		return Snapshot{Leases: map[string]LeaseRecord{}}, nil
	}

	var snapshot Snapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decode state file: %w", err)
	}
	if snapshot.Leases == nil {
		snapshot.Leases = map[string]LeaseRecord{}
	}

	return snapshot, nil
}

func (s *FileStore) save(snapshot Snapshot) error {
	if snapshot.Leases == nil {
		snapshot.Leases = map[string]LeaseRecord{}
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state file: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), "state-*.json")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}

	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}

	return nil
}
