package storage

import (
	"fmt"
	"sync"
	"time"
)

// Lock represents a successfully acquired lease.
type Lock struct {
	Key          string    `json:"key"`
	Owner        string    `json:"owner"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// LockBackend defines the interface for distributed lock backends.
type LockBackend interface {
	Acquire(key string, owner string, ttl time.Duration) (*Lock, error)
	Release(key string, owner string) (bool, error)
	Renew(key string, owner string, ttl time.Duration) (bool, error)
	Get(key string) (*Lock, error)
}

// InMemoryStore is a thread-safe, local implementation of LockBackend.
type InMemoryStore struct {
	mu           sync.Mutex
	locks        map[string]*Lock
	tokenCounter int64
}

// NewInMemoryStore initializes and returns a local LockBackend.
func NewInMemoryStore() *InMemoryStore {
	store := &InMemoryStore{
		locks: make(map[string]*Lock),
	}
	go store.startExpiryCleaner(1 * time.Second)
	return store
}

// Acquire requests a lock for a key. Returns error if already acquired and not expired.
func (s *InMemoryStore) Acquire(key string, owner string, ttl time.Duration) (*Lock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	existing, exists := s.locks[key]
	if exists && existing.ExpiresAt.After(now) {
		if existing.Owner == owner {
			// Reentrant behavior: extend lock duration
			existing.ExpiresAt = now.Add(ttl)
			return existing, nil
		}
		return nil, fmt.Errorf("lock for key %q is held by owner %q", key, existing.Owner)
	}

	s.tokenCounter++
	lock := &Lock{
		Key:          key,
		Owner:        owner,
		FencingToken: s.tokenCounter,
		ExpiresAt:    now.Add(ttl),
	}
	s.locks[key] = lock
	return lock, nil
}

// Release frees the lock if the requester matches the owner.
func (s *InMemoryStore) Release(key string, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[key]
	if !exists || existing.ExpiresAt.Before(time.Now()) {
		return false, nil
	}

	if existing.Owner != owner {
		return false, fmt.Errorf("cannot release lock owned by %q", existing.Owner)
	}

	delete(s.locks, key)
	return true, nil
}

// Renew extends the lock lease if active and owned by the requestor.
func (s *InMemoryStore) Renew(key string, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[key]
	if !exists || existing.ExpiresAt.Before(time.Now()) {
		return false, fmt.Errorf("lock has expired or does not exist")
	}

	if existing.Owner != owner {
		return false, fmt.Errorf("cannot renew lock owned by %q", existing.Owner)
	}

	existing.ExpiresAt = time.Now().Add(ttl)
	return true, nil
}

// Get retrieves current active lock details.
func (s *InMemoryStore) Get(key string) (*Lock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[key]
	if !exists || existing.ExpiresAt.Before(time.Now()) {
		return nil, fmt.Errorf("lock not found or expired")
	}
	return existing, nil
}

func (s *InMemoryStore) startExpiryCleaner(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for k, v := range s.locks {
			if v.ExpiresAt.Before(now) {
				delete(s.locks, k)
			}
		}
		s.mu.Unlock()
	}
}
