package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"servlock/pkg/storage"
)

func TestLockLifecycle(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// 1. Acquire Lock
	payload1 := LockRequest{
		Key:      "resource-A",
		Owner:    "client-1",
		Duration: 1000,
	}
	body, _ := json.Marshal(payload1)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(body))
	rr1 := httptest.NewRecorder()

	HandleAcquireLock(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr1.Code)
	}

	var resp1 LockResponse
	json.NewDecoder(rr1.Body).Decode(&resp1)
	if resp1.Status != "success" || resp1.Lock == nil {
		t.Fatalf("expected success status and non-nil lock: %+v", resp1)
	}

	if resp1.Lock.Key != "resource-A" || resp1.Lock.Owner != "client-1" {
		t.Errorf("unexpected lock details: %+v", resp1.Lock)
	}

	// 2. Try acquire held lock (expected status Conflict)
	payload2 := LockRequest{
		Key:      "resource-A",
		Owner:    "client-2",
		Duration: 1000,
	}
	body2, _ := json.Marshal(payload2)
	req2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(body2))
	rr2 := httptest.NewRecorder()

	HandleAcquireLock(rr2, req2)
	if rr2.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict, got %d", rr2.Code)
	}

	// 3. Renew Lock
	req3 := httptest.NewRequest("POST", "/api/locks/renew", bytes.NewReader(body))
	rr3 := httptest.NewRecorder()

	HandleRenewLock(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr3.Code)
	}

	// 4. Release Lock
	req4 := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(body))
	rr4 := httptest.NewRecorder()

	HandleReleaseLock(rr4, req4)
	if rr4.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr4.Code)
	}

	// 5. Try release already released lock
	req5 := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(body))
	rr5 := httptest.NewRecorder()

	HandleReleaseLock(rr5, req5)
	var resp5 LockResponse
	json.NewDecoder(rr5.Body).Decode(&resp5)
	if resp5.Status != "failed" {
		t.Errorf("expected release to fail, got status %q", resp5.Status)
	}
}

func TestLockExpiration(t *testing.T) {
	Store = storage.NewInMemoryStore()

	payload := LockRequest{
		Key:      "resource-B",
		Owner:    "client-1",
		Duration: 50, // 50 milliseconds
	}
	body, _ := json.Marshal(payload)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(body))
	rr1 := httptest.NewRecorder()

	HandleAcquireLock(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr1.Code)
	}

	time.Sleep(100 * time.Millisecond) // wait for lock lease to expire

	// Acquire again by different owner should succeed now
	payload2 := LockRequest{
		Key:      "resource-B",
		Owner:    "client-2",
		Duration: 1000,
	}
	body2, _ := json.Marshal(payload2)
	req2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(body2))
	rr2 := httptest.NewRecorder()

	HandleAcquireLock(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Errorf("expected 200 OK after expiration, got %d", rr2.Code)
	}
}

func TestLockQueueingAndFairness(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// 1. Acquire Lock for client-1
	p1 := LockRequest{
		Key:      "resource-Q",
		Owner:    "client-1",
		Duration: 1000,
	}
	b1, _ := json.Marshal(p1)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b1))
	rr1 := httptest.NewRecorder()
	HandleAcquireLock(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for client-1, got %d", rr1.Code)
	}

	// 2. Start waiter client-2 in background (wait_ms = 500)
	errChan := make(chan error, 2)
	go func() {
		p2 := LockRequest{
			Key:      "resource-Q",
			Owner:    "client-2",
			Duration: 1000,
			WaitTime: 500,
		}
		b2, _ := json.Marshal(p2)
		req2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b2))
		rr2 := httptest.NewRecorder()
		HandleAcquireLock(rr2, req2)
		if rr2.Code != http.StatusOK {
			errChan <- fmt.Errorf("client-2: expected 200 OK, got %d", rr2.Code)
			return
		}
		var resp LockResponse
		json.NewDecoder(rr2.Body).Decode(&resp)
		if resp.Status != "success" || resp.Lock.Owner != "client-2" {
			errChan <- fmt.Errorf("client-2: expected lock granted to client-2, got %+v", resp)
			return
		}
		errChan <- nil
	}()

	// Wait to ensure client-2 is queued first
	time.Sleep(50 * time.Millisecond)

	// 3. Start waiter client-3 in background (wait_ms = 500)
	go func() {
		p3 := LockRequest{
			Key:      "resource-Q",
			Owner:    "client-3",
			Duration: 1000,
			WaitTime: 500,
		}
		b3, _ := json.Marshal(p3)
		req3 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b3))
		rr3 := httptest.NewRecorder()
		HandleAcquireLock(rr3, req3)
		if rr3.Code != http.StatusOK {
			errChan <- fmt.Errorf("client-3: expected 200 OK, got %d", rr3.Code)
			return
		}
		var resp LockResponse
		json.NewDecoder(rr3.Body).Decode(&resp)
		if resp.Status != "success" || resp.Lock.Owner != "client-3" {
			errChan <- fmt.Errorf("client-3: expected lock granted to client-3, got %+v", resp)
			return
		}
		errChan <- nil
	}()

	// Give background threads a short moment to enter the wait queue
	time.Sleep(50 * time.Millisecond)

	// 4. Release lock by client-1. This should instantly grant it to client-2 (first in queue).
	reqRelease := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(b1))
	rrRelease := httptest.NewRecorder()
	HandleReleaseLock(rrRelease, reqRelease)
	if rrRelease.Code != http.StatusOK {
		t.Fatalf("expected release 200 OK, got %d", rrRelease.Code)
	}

	// Wait for client-2 to finish and check error
	err2 := <-errChan
	if err2 != nil {
		t.Fatal(err2)
	}

	// 5. Release lock by client-2. This should grant it to client-3 (next in queue).
	p2Release := LockRequest{
		Key:   "resource-Q",
		Owner: "client-2",
	}
	b2Release, _ := json.Marshal(p2Release)
	reqRelease2 := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(b2Release))
	rrRelease2 := httptest.NewRecorder()
	HandleReleaseLock(rrRelease2, reqRelease2)
	if rrRelease2.Code != http.StatusOK {
		t.Fatalf("expected release 2 200 OK, got %d, body: %s", rrRelease2.Code, rrRelease2.Body.String())
	}

	// Wait for client-3 to finish and check error
	err3 := <-errChan
	if err3 != nil {
		t.Fatal(err3)
	}
}

func TestLockObservability(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// 1. Acquire lock
	p1 := LockRequest{
		Key:      "resource-Obs",
		Owner:    "owner-1",
		Duration: 5000,
	}
	b1, _ := json.Marshal(p1)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b1))
	rr1 := httptest.NewRecorder()
	HandleAcquireLock(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr1.Code)
	}

	// 2. Start two waiters in background
	go func() {
		p2 := LockRequest{
			Key:      "resource-Obs",
			Owner:    "waiter-1",
			Duration: 5000,
			WaitTime: 2000,
		}
		b2, _ := json.Marshal(p2)
		req2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b2))
		rr2 := httptest.NewRecorder()
		HandleAcquireLock(rr2, req2)
	}()

	go func() {
		p3 := LockRequest{
			Key:      "resource-Obs",
			Owner:    "waiter-2",
			Duration: 5000,
			WaitTime: 2000,
		}
		b3, _ := json.Marshal(p3)
		req3 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b3))
		rr3 := httptest.NewRecorder()
		HandleAcquireLock(rr3, req3)
	}()

	// Wait for queue entry
	time.Sleep(50 * time.Millisecond)

	// 3. Call observability endpoint
	reqObs := httptest.NewRequest("GET", "/api/locks/observability", nil)
	rrObs := httptest.NewRecorder()
	HandleLockObservability(rrObs, reqObs)
	if rrObs.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for observability, got %d", rrObs.Code)
	}

	var locks []storage.LockInfo
	json.NewDecoder(rrObs.Body).Decode(&locks)

	if len(locks) != 1 {
		t.Fatalf("expected exactly 1 lock, got %d", len(locks))
	}
	lock := locks[0]
	if lock.Key != "resource-Obs" || lock.Owner != "owner-1" {
		t.Errorf("unexpected lock owner/key: %+v", lock)
	}
	if len(lock.Waiters) != 2 {
		t.Errorf("expected 2 waiters, got %d: %+v", len(lock.Waiters), lock.Waiters)
	}
}
