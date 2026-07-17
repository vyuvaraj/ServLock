package storage

import (
	"encoding/json"
	"os"
	"time"
)

// FileLockStore implements local file-persisted locking.
type FileLockStore struct {
	*InMemoryStore
	filePath string
}

func NewFileLockStore(filePath string) (*FileLockStore, error) {
	ims := NewInMemoryStore()
	store := &FileLockStore{
		InMemoryStore: ims,
		filePath:      filePath,
	}
	if err := store.loadFromFile(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileLockStore) loadFromFile() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var savedLocks map[string]*Lock
	if err := json.Unmarshal(data, &savedLocks); err != nil {
		return err
	}

	now := time.Now()
	for k, lock := range savedLocks {
		if lock.ExpiresAt.After(now) {
			s.locks[k] = lock
			if lock.FencingToken > s.tokenCounter {
				s.tokenCounter = lock.FencingToken
			}
		}
	}
	return nil
}

func (s *FileLockStore) saveToFile() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(s.locks)
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath, data, 0600)
}

func (s *FileLockStore) Acquire(key string, owner string, clientID string, ttl time.Duration) (*Lock, error) {
	return s.AcquireAdvanced(key, owner, clientID, ttl, "exclusive")
}

func (s *FileLockStore) AcquireWithWait(key string, owner string, clientID string, ttl time.Duration, waitTimeout time.Duration) (*Lock, error) {
	return s.AcquireAdvancedWithWait(key, owner, clientID, ttl, waitTimeout, 0, "exclusive")
}

func (s *FileLockStore) AcquireAdvanced(key string, owner string, clientID string, ttl time.Duration, mode string) (*Lock, error) {
	lock, err := s.InMemoryStore.AcquireAdvanced(key, owner, clientID, ttl, mode)
	if err == nil {
		s.saveToFile()
	}
	return lock, err
}

func (s *FileLockStore) AcquireAdvancedWithWait(key string, owner string, clientID string, ttl time.Duration, waitTimeout time.Duration, priority int, mode string) (*Lock, error) {
	lock, err := s.InMemoryStore.AcquireAdvancedWithWait(key, owner, clientID, ttl, waitTimeout, priority, mode)
	if err == nil {
		s.saveToFile()
	}
	return lock, err
}

func (s *FileLockStore) Release(key string, owner string, fencingToken int64) (bool, error) {
	ok, err := s.InMemoryStore.Release(key, owner, fencingToken)
	if ok && err == nil {
		s.saveToFile()
	}
	return ok, err
}

func (s *FileLockStore) Renew(key string, owner string, fencingToken int64, ttl time.Duration) (bool, error) {
	ok, err := s.InMemoryStore.Renew(key, owner, fencingToken, ttl)
	if ok && err == nil {
		s.saveToFile()
	}
	return ok, err
}
