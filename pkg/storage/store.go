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

type waiter struct {
	owner   string
	ttl     time.Duration
	granted chan *Lock
}

// LockBackend defines the interface for distributed lock backends.
type LockBackend interface {
	Acquire(key string, owner string, ttl time.Duration) (*Lock, error)
	AcquireWithWait(key string, owner string, ttl time.Duration, waitTimeout time.Duration) (*Lock, error)
	Release(key string, owner string) (bool, error)
	Renew(key string, owner string, ttl time.Duration) (bool, error)
	Get(key string) (*Lock, error)
}

// InMemoryStore is a thread-safe, local implementation of LockBackend.
type InMemoryStore struct {
	mu           sync.Mutex
	locks        map[string]*Lock
	waiters      map[string][]*waiter
	tokenCounter int64
}

// NewInMemoryStore initializes and returns a local LockBackend.
func NewInMemoryStore() *InMemoryStore {
	store := &InMemoryStore{
		locks:   make(map[string]*Lock),
		waiters: make(map[string][]*waiter),
	}
	go store.startExpiryCleaner(1 * time.Second)
	return store
}

// Acquire requests a lock for a key. Returns error if already acquired and not expired.
func (s *InMemoryStore) Acquire(key string, owner string, ttl time.Duration) (*Lock, error) {
	return s.AcquireWithWait(key, owner, ttl, 0)
}

// AcquireWithWait requests a lock for a key. Blocks up to waitTimeout if the lock is held.
func (s *InMemoryStore) AcquireWithWait(key string, owner string, ttl time.Duration, waitTimeout time.Duration) (*Lock, error) {
	s.mu.Lock()
	now := time.Now()
	existing, exists := s.locks[key]
	if exists && existing.ExpiresAt.After(now) {
		if existing.Owner == owner {
			// Reentrant behavior: extend lock duration
			existing.ExpiresAt = now.Add(ttl)
			s.mu.Unlock()
			return existing, nil
		}

		if waitTimeout <= 0 {
			s.mu.Unlock()
			return nil, fmt.Errorf("lock for key %q is held by owner %q", key, existing.Owner)
		}

		// Queue the waiter
		w := &waiter{
			owner:   owner,
			ttl:     ttl,
			granted: make(chan *Lock, 1),
		}
		s.waiters[key] = append(s.waiters[key], w)
		s.mu.Unlock()

		select {
		case lock := <-w.granted:
			return lock, nil
		case <-time.After(waitTimeout):
			// Timeout expired: remove from waiter queue
			s.mu.Lock()
			q := s.waiters[key]
			for i, val := range q {
				if val == w {
					s.waiters[key] = append(q[:i], q[i+1:]...)
					break
				}
			}
			s.mu.Unlock()
			return nil, fmt.Errorf("timeout waiting for lock %q", key)
		}
	}

	s.tokenCounter++
	lock := &Lock{
		Key:          key,
		Owner:        owner,
		FencingToken: s.tokenCounter,
		ExpiresAt:    now.Add(ttl),
	}
	s.locks[key] = lock
	s.mu.Unlock()
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
	s.grantNextWaiterLocked(key)
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

func (s *InMemoryStore) grantNextWaiterLocked(key string) {
	q, exists := s.waiters[key]
	if !exists || len(q) == 0 {
		return
	}

	// Pop the first waiter
	w := q[0]
	if len(q) > 1 {
		s.waiters[key] = q[1:]
	} else {
		delete(s.waiters, key)
	}

	s.tokenCounter++
	lock := &Lock{
		Key:          key,
		Owner:        w.owner,
		FencingToken: s.tokenCounter,
		ExpiresAt:    time.Now().Add(w.ttl),
	}
	s.locks[key] = lock
	w.granted <- lock
}

func (s *InMemoryStore) startExpiryCleaner(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for k, v := range s.locks {
			if v.ExpiresAt.Before(now) {
				delete(s.locks, k)
				s.grantNextWaiterLocked(k)
			}
		}
		s.mu.Unlock()
	}
}

type LockInfo struct {
	Key          string    `json:"key"`
	Owner        string    `json:"owner"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	Waiters      []string  `json:"waiters"`
}

func (s *InMemoryStore) GetActiveLocks() []LockInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res []LockInfo
	now := time.Now()
	for k, lock := range s.locks {
		if lock.ExpiresAt.After(now) {
			var waitList []string
			for _, w := range s.waiters[k] {
				waitList = append(waitList, w.owner)
			}
			res = append(res, LockInfo{
				Key:          lock.Key,
				Owner:        lock.Owner,
				FencingToken: lock.FencingToken,
				ExpiresAt:    lock.ExpiresAt,
				Waiters:      waitList,
			})
		}
	}
	return res
}
