package storage

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Lock represents a successfully acquired lease.
type Lock struct {
	Key             string           `json:"key"`
	Owner           string           `json:"owner"`
	ClientID        string           `json:"client_id"`
	ReentrancyCount int              `json:"reentrancy_count"`
	FencingToken    int64            `json:"fencing_token"`
	ExpiresAt       time.Time        `json:"expires_at"`
	Mode            string           `json:"mode"`                    // "shared" or "exclusive"
	SharedOwners    map[string]*Lock `json:"shared_owners,omitempty"` // other shared owners
	AcquiredAt      time.Time        `json:"acquired_at"`
}

type waiter struct {
	owner    string
	clientID string
	ttl      time.Duration
	priority int
	mode     string
	granted  chan *Lock
}

type LockEvent struct {
	Key    string `json:"key"`
	Action string `json:"action"` // "released" or "expired"
}

// LockBackend defines the interface for distributed lock backends.
type LockBackend interface {
	Acquire(key string, owner string, clientID string, ttl time.Duration) (*Lock, error)
	AcquireWithWait(key string, owner string, clientID string, ttl time.Duration, waitTimeout time.Duration) (*Lock, error)
	AcquireAdvanced(key string, owner string, clientID string, ttl time.Duration, mode string) (*Lock, error)
	AcquireAdvancedWithWait(key string, owner string, clientID string, ttl time.Duration, waitTimeout time.Duration, priority int, mode string) (*Lock, error)
	Release(key string, owner string, fencingToken int64) (bool, error)
	Renew(key string, owner string, fencingToken int64, ttl time.Duration) (bool, error)
	Get(key string) (*Lock, error)
	Subscribe() chan LockEvent
	Unsubscribe(chan LockEvent)
	PingHeartbeat(clientID string)
	ExpireHeartbeatForTest(clientID string)
}

// InMemoryStore is a thread-safe, local implementation of LockBackend.
type InMemoryStore struct {
	mu            sync.Mutex
	locks         map[string]*Lock
	waiters       map[string][]*waiter
	waitingFor    map[string]string // owner -> key
	tokenCounter  int64
	deadlockCount int64

	// Pub/Sub
	listenersMu sync.Mutex
	listeners   []chan LockEvent

	// Heartbeats
	heartbeats map[string]time.Time
}

// NewInMemoryStore initializes and returns a local LockBackend.
func NewInMemoryStore() *InMemoryStore {
	store := &InMemoryStore{
		locks:      make(map[string]*Lock),
		waiters:    make(map[string][]*waiter),
		waitingFor: make(map[string]string),
		heartbeats: make(map[string]time.Time),
	}
	go store.startExpiryCleaner(500 * time.Millisecond)
	return store
}

func (s *InMemoryStore) Subscribe() chan LockEvent {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	ch := make(chan LockEvent, 10)
	s.listeners = append(s.listeners, ch)
	return ch
}

func (s *InMemoryStore) Unsubscribe(ch chan LockEvent) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	for i, l := range s.listeners {
		if l == ch {
			s.listeners = append(s.listeners[:i], s.listeners[i+1:]...)
			close(ch)
			break
		}
	}
}

func (s *InMemoryStore) broadcast(event LockEvent) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	for _, ch := range s.listeners {
		select {
		case ch <- event:
		default:
			// If channel buffer is full, drop event to prevent blocking
		}
	}
}

func (s *InMemoryStore) PingHeartbeat(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats[clientID] = time.Now()
}

func (s *InMemoryStore) ExpireHeartbeatForTest(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats[clientID] = time.Now().Add(-10 * time.Second)
}

// Acquire requests a lock for a key. Returns error if already acquired and not expired.
func (s *InMemoryStore) Acquire(key string, owner string, clientID string, ttl time.Duration) (*Lock, error) {
	return s.AcquireAdvancedWithWait(key, owner, clientID, ttl, 0, 0, "exclusive")
}

// AcquireWithWait requests a lock for a key. Blocks up to waitTimeout if the lock is held.
func (s *InMemoryStore) AcquireWithWait(key string, owner string, clientID string, ttl time.Duration, waitTimeout time.Duration) (*Lock, error) {
	return s.AcquireAdvancedWithWait(key, owner, clientID, ttl, waitTimeout, 0, "exclusive")
}

func (s *InMemoryStore) AcquireAdvanced(key string, owner string, clientID string, ttl time.Duration, mode string) (*Lock, error) {
	return s.AcquireAdvancedWithWait(key, owner, clientID, ttl, 0, 0, mode)
}

// AcquireAdvancedWithWait requests a lock for a key. Blocks up to waitTimeout if the lock is held.
func (s *InMemoryStore) AcquireAdvancedWithWait(key string, owner string, clientID string, ttl time.Duration, waitTimeout time.Duration, priority int, mode string) (*Lock, error) {
	s.mu.Lock()
	now := time.Now()
	if mode == "" {
		mode = "exclusive"
	}

	existing, exists := s.locks[key]
	if exists && existing.ExpiresAt.After(now) {
		// Reentrancy Check
		if existing.Owner == owner && existing.ClientID == clientID {
			existing.ReentrancyCount++
			existing.ExpiresAt = now.Add(ttl)
			s.mu.Unlock()
			return existing, nil
		}
		if existing.Mode == "shared" && existing.SharedOwners != nil {
			if sh, found := existing.SharedOwners[owner]; found && sh.ClientID == clientID {
				sh.ReentrancyCount++
				sh.ExpiresAt = now.Add(ttl)
				if sh.ExpiresAt.After(existing.ExpiresAt) {
					existing.ExpiresAt = sh.ExpiresAt
				}
				s.mu.Unlock()
				return sh, nil
			}
		}

		// Check Lock Mode compatibility
		conflict := false
		if mode == "exclusive" || existing.Mode == "exclusive" {
			conflict = true
		}

		if conflict {
			if waitTimeout <= 0 {
				s.mu.Unlock()
				return nil, fmt.Errorf("lock conflict: key %q is held in mode %q", key, existing.Mode)
			}

			if s.hasCycleLocked(existing.Owner, owner) {
				s.deadlockCount++
				s.mu.Unlock()
				return nil, fmt.Errorf("deadlock detected: cycle in lock wait queue")
			}

			w := &waiter{
				owner:    owner,
				clientID: clientID,
				ttl:      ttl,
				priority: priority,
				mode:     mode,
				granted:  make(chan *Lock, 1),
			}
			s.waiters[key] = append(s.waiters[key], w)
			// Sort waiters by priority descending
			sort.Slice(s.waiters[key], func(i, j int) bool {
				return s.waiters[key][i].priority > s.waiters[key][j].priority
			})

			s.waitingFor[owner] = key
			s.mu.Unlock()

			select {
			case lock := <-w.granted:
				s.mu.Lock()
				delete(s.waitingFor, owner)
				s.mu.Unlock()
				return lock, nil
			case <-time.After(waitTimeout):
				s.mu.Lock()
				delete(s.waitingFor, owner)
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
	}

	s.tokenCounter++
	lock := &Lock{
		Key:             key,
		Owner:           owner,
		ClientID:        clientID,
		ReentrancyCount: 1,
		FencingToken:    s.tokenCounter,
		ExpiresAt:       now.Add(ttl),
		Mode:            mode,
		AcquiredAt:      now,
	}

	if mode == "shared" {
		lock.SharedOwners = make(map[string]*Lock)
		lock.SharedOwners[owner] = lock
	}

	if exists && existing.Mode == "shared" && mode == "shared" {
		existing.SharedOwners[owner] = lock
		if lock.ExpiresAt.After(existing.ExpiresAt) {
			existing.ExpiresAt = lock.ExpiresAt
		}
		s.mu.Unlock()
		return lock, nil
	}

	s.locks[key] = lock
	s.mu.Unlock()
	s.replicateState()
	return lock, nil
}

func (s *InMemoryStore) hasCycleLocked(startOwner, targetOwner string) bool {
	visited := make(map[string]bool)
	visited[targetOwner] = true

	current := startOwner
	for current != "" {
		if visited[current] {
			if current == targetOwner {
				return true
			}
			break
		}
		visited[current] = true

		waitKey, waiting := s.waitingFor[current]
		if !waiting {
			break
		}

		lock, exists := s.locks[waitKey]
		if !exists || lock.ExpiresAt.Before(time.Now()) {
			break
		}

		current = lock.Owner
	}
	return false
}

// Release frees the lock if the requester matches the owner.
func (s *InMemoryStore) Release(key string, owner string, fencingToken int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[key]
	if !exists || existing.ExpiresAt.Before(time.Now()) {
		return false, nil
	}

	if existing.Mode == "shared" && existing.SharedOwners != nil {
		sh, found := existing.SharedOwners[owner]
		if !found {
			return false, fmt.Errorf("cannot release lock not owned by %q", owner)
		}
		sh.ReentrancyCount--
		if sh.ReentrancyCount > 0 {
			return true, nil
		}
		delete(existing.SharedOwners, owner)
		if len(existing.SharedOwners) > 0 {
			var maxExp time.Time
			for _, shL := range existing.SharedOwners {
				if shL.ExpiresAt.After(maxExp) {
					maxExp = shL.ExpiresAt
				}
			}
			existing.ExpiresAt = maxExp
			return true, nil
		}
	} else {
		if existing.Owner != owner {
			return false, fmt.Errorf("cannot release lock owned by %q", existing.Owner)
		}

		if fencingToken > 0 && existing.FencingToken != fencingToken {
			return false, fmt.Errorf("fencing token mismatch (expected %d, got %d)", existing.FencingToken, fencingToken)
		}

		existing.ReentrancyCount--
		if existing.ReentrancyCount > 0 {
			return true, nil
		}
	}

	holdTime := time.Since(existing.AcquiredAt)
	if holdTime > 5*time.Second {
		log.Printf("[Warning] Zombie Lock Alert: Lock on %q was held for %v", key, holdTime)
	}

	delete(s.locks, key)
	s.grantNextWaiterLocked(key)
	s.broadcast(LockEvent{Key: key, Action: "released"})
	s.replicateState()
	return true, nil
}

// Renew extends the lock lease if active and owned by the requestor.
func (s *InMemoryStore) Renew(key string, owner string, fencingToken int64, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[key]
	if !exists || existing.ExpiresAt.Before(time.Now()) {
		return false, fmt.Errorf("lock has expired or does not exist")
	}

	if existing.Mode == "shared" && existing.SharedOwners != nil {
		sh, found := existing.SharedOwners[owner]
		if !found {
			return false, fmt.Errorf("cannot renew lock not owned by %q", owner)
		}
		sh.ExpiresAt = time.Now().Add(ttl)
		if sh.ExpiresAt.After(existing.ExpiresAt) {
			existing.ExpiresAt = sh.ExpiresAt
		}
		return true, nil
	}

	if existing.Owner != owner {
		return false, fmt.Errorf("cannot renew lock owned by %q", existing.Owner)
	}

	if fencingToken > 0 && existing.FencingToken != fencingToken {
		return false, fmt.Errorf("fencing token mismatch (expected %d, got %d)", existing.FencingToken, fencingToken)
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

	w := q[0]
	if len(q) > 1 {
		s.waiters[key] = q[1:]
	} else {
		delete(s.waiters, key)
	}

	delete(s.waitingFor, w.owner)

	s.tokenCounter++
	lock := &Lock{
		Key:             key,
		Owner:           w.owner,
		ClientID:        w.clientID,
		ReentrancyCount: 1,
		FencingToken:    s.tokenCounter,
		ExpiresAt:       time.Now().Add(w.ttl),
		Mode:            w.mode,
		AcquiredAt:      time.Now(),
	}
	if w.mode == "shared" {
		lock.SharedOwners = make(map[string]*Lock)
		lock.SharedOwners[w.owner] = lock
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
				s.broadcast(LockEvent{Key: k, Action: "expired"})
			}
		}

		for cid, t := range s.heartbeats {
			if now.Sub(t) > 5*time.Second {
				delete(s.heartbeats, cid)
				log.Printf("[Heartbeat] Client %q disconnected. Releasing held leases.", cid)
				for k, v := range s.locks {
					if v.ClientID == cid {
						delete(s.locks, k)
						s.grantNextWaiterLocked(k)
						s.broadcast(LockEvent{Key: k, Action: "expired"})
					} else if v.Mode == "shared" && v.SharedOwners != nil {
						for owner, shL := range v.SharedOwners {
							if shL.ClientID == cid {
								delete(v.SharedOwners, owner)
							}
						}
						if len(v.SharedOwners) == 0 {
							delete(s.locks, k)
							s.grantNextWaiterLocked(k)
							s.broadcast(LockEvent{Key: k, Action: "expired"})
						}
					}
				}
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

func (s *InMemoryStore) GetDeadlockCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadlockCount
}

// FileLockStore implements local file-persisted locking.

