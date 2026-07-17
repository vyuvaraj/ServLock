package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

func TestReentrantLock(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// 1. First acquire (reentrancy_count should be 1)
	p1 := LockRequest{
		Key:      "resource-reentrant",
		Owner:    "owner-1",
		ClientID: "client-abc",
		Duration: 5000,
	}
	b1, _ := json.Marshal(p1)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b1))
	rr1 := httptest.NewRecorder()
	HandleAcquireLock(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr1.Code)
	}

	var resp1 LockResponse
	json.NewDecoder(rr1.Body).Decode(&resp1)
	if resp1.Lock.ReentrancyCount != 1 {
		t.Errorf("expected reentrancy_count 1, got %d", resp1.Lock.ReentrancyCount)
	}

	// 2. Second acquire with same client_id (should succeed, count should be 2)
	req2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b1))
	rr2 := httptest.NewRecorder()
	HandleAcquireLock(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr2.Code)
	}

	var resp2 LockResponse
	json.NewDecoder(rr2.Body).Decode(&resp2)
	if resp2.Lock.ReentrancyCount != 2 {
		t.Errorf("expected reentrancy_count 2, got %d", resp2.Lock.ReentrancyCount)
	}

	// 3. Try acquire with different client_id (should fail/conflict)
	p3 := LockRequest{
		Key:      "resource-reentrant",
		Owner:    "owner-1",
		ClientID: "client-xyz",
		Duration: 5000,
	}
	b3, _ := json.Marshal(p3)
	req3 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b3))
	rr3 := httptest.NewRecorder()
	HandleAcquireLock(rr3, req3)
	if rr3.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict, got %d", rr3.Code)
	}

	// 4. First release (should succeed, count decrements to 1, lock still active)
	reqRelease1 := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(b1))
	rrRelease1 := httptest.NewRecorder()
	HandleReleaseLock(rrRelease1, reqRelease1)
	if rrRelease1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rrRelease1.Code)
	}

	activeLock, err := Store.Get("resource-reentrant")
	if err != nil || activeLock == nil {
		t.Fatalf("expected lock to still be active after first release")
	}
	if activeLock.ReentrancyCount != 1 {
		t.Errorf("expected active lock reentrancy_count 1, got %d", activeLock.ReentrancyCount)
	}

	// 5. Second release (should release the lock completely)
	reqRelease2 := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(b1))
	rrRelease2 := httptest.NewRecorder()
	HandleReleaseLock(rrRelease2, reqRelease2)
	if rrRelease2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rrRelease2.Code)
	}

	_, err = Store.Get("resource-reentrant")
	if err == nil {
		t.Errorf("expected lock to be fully released/deleted")
	}
}

func TestFencingToken(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// 1. Acquire Lock
	p1 := LockRequest{
		Key:      "resource-fencing",
		Owner:    "owner-1",
		Duration: 5000,
	}
	b1, _ := json.Marshal(p1)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b1))
	rr1 := httptest.NewRecorder()
	HandleAcquireLock(rr1, req1)
	
	var resp1 LockResponse
	json.NewDecoder(rr1.Body).Decode(&resp1)
	token := resp1.Lock.FencingToken

	if token <= 0 {
		t.Fatalf("expected positive fencing token, got %d", token)
	}

	// 2. Try to renew with wrong fencing token (should fail)
	pRenewWrong := LockRequest{
		Key:          "resource-fencing",
		Owner:        "owner-1",
		FencingToken: token + 99,
		Duration:     5000,
	}
	bRenewWrong, _ := json.Marshal(pRenewWrong)
	reqRenewWrong := httptest.NewRequest("POST", "/api/locks/renew", bytes.NewReader(bRenewWrong))
	rrRenewWrong := httptest.NewRecorder()
	HandleRenewLock(rrRenewWrong, reqRenewWrong)
	if rrRenewWrong.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found (or renewal failed), got %d. Body: %s", rrRenewWrong.Code, rrRenewWrong.Body.String())
	}

	// 3. Renew with correct fencing token (should succeed)
	pRenewCorrect := LockRequest{
		Key:          "resource-fencing",
		Owner:        "owner-1",
		FencingToken: token,
		Duration:     5000,
	}
	bRenewCorrect, _ := json.Marshal(pRenewCorrect)
	reqRenewCorrect := httptest.NewRequest("POST", "/api/locks/renew", bytes.NewReader(bRenewCorrect))
	rrRenewCorrect := httptest.NewRecorder()
	HandleRenewLock(rrRenewCorrect, reqRenewCorrect)
	if rrRenewCorrect.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d. Body: %s", rrRenewCorrect.Code, rrRenewCorrect.Body.String())
	}

	// 4. Try release with wrong fencing token (should fail)
	pReleaseWrong := LockRequest{
		Key:          "resource-fencing",
		Owner:        "owner-1",
		FencingToken: token + 99,
	}
	bReleaseWrong, _ := json.Marshal(pReleaseWrong)
	reqReleaseWrong := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(bReleaseWrong))
	rrReleaseWrong := httptest.NewRecorder()
	HandleReleaseLock(rrReleaseWrong, reqReleaseWrong)
	if rrReleaseWrong.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rrReleaseWrong.Code)
	}

	// 5. Release with correct fencing token (should succeed)
	pReleaseCorrect := LockRequest{
		Key:          "resource-fencing",
		Owner:        "owner-1",
		FencingToken: token,
	}
	bReleaseCorrect, _ := json.Marshal(pReleaseCorrect)
	reqReleaseCorrect := httptest.NewRequest("POST", "/api/locks/release", bytes.NewReader(bReleaseCorrect))
	rrReleaseCorrect := httptest.NewRecorder()
	HandleReleaseLock(rrReleaseCorrect, reqReleaseCorrect)
	if rrReleaseCorrect.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rrReleaseCorrect.Code)
	}
}

func TestDeadlockDetection(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// Setup: Client A holds Lock 1
	p1 := LockRequest{Key: "lock-1", Owner: "client-A", Duration: 10000}
	b1, _ := json.Marshal(p1)
	req1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b1))
	HandleAcquireLock(httptest.NewRecorder(), req1)

	// Setup: Client B holds Lock 2
	p2 := LockRequest{Key: "lock-2", Owner: "client-B", Duration: 10000}
	b2, _ := json.Marshal(p2)
	req2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b2))
	HandleAcquireLock(httptest.NewRecorder(), req2)

	// Make Client A wait for Lock 2 (in background)
	go func() {
		pWait2 := LockRequest{Key: "lock-2", Owner: "client-A", Duration: 10000, WaitTime: 5000}
		bw2, _ := json.Marshal(pWait2)
		reqWait2 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(bw2))
		HandleAcquireLock(httptest.NewRecorder(), reqWait2)
	}()

	// Wait for queue entry
	time.Sleep(50 * time.Millisecond)

	// Now: Client B tries to acquire Lock 1 (which Client A holds).
	// This would create a circular wait: Client A waiting for Client B (Lock 2),
	// and Client B waiting for Client A (Lock 1). Deadlock detected!
	pWait1 := LockRequest{Key: "lock-1", Owner: "client-B", Duration: 10000, WaitTime: 5000}
	bw1, _ := json.Marshal(pWait1)
	reqWait1 := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(bw1))
	rrWait1 := httptest.NewRecorder()
	HandleAcquireLock(rrWait1, reqWait1)

	if rrWait1.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict, got %d", rrWait1.Code)
	}

	var resp LockResponse
	json.NewDecoder(rrWait1.Body).Decode(&resp)
	if !strings.Contains(resp.Message, "deadlock detected") {
		t.Errorf("expected deadlock error message, got %q", resp.Message)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	Store = storage.NewInMemoryStore()

	req := httptest.NewRequest("GET", "/api/locks/metrics", nil)
	rr := httptest.NewRecorder()
	HandleMetrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	body := rr.Body.String()
	if !strings.Contains(body, "servlock_active_locks") {
		t.Errorf("expected metric 'servlock_active_locks' in body, got: %s", body)
	}
	if !strings.Contains(body, "servlock_deadlocks_total") {
		t.Errorf("expected metric 'servlock_deadlocks_total' in body, got: %s", body)
	}
}

func TestFileLockPersistence(t *testing.T) {
	tempFile := "test_leases.json"
	defer os.Remove(tempFile)

	// Create a new FileLockStore and acquire a lock
	fileStore, err := storage.NewFileLockStore(tempFile)
	if err != nil {
		t.Fatalf("failed to create FileLockStore: %v", err)
	}
	Store = fileStore

	p := LockRequest{
		Key:      "persisted-resource",
		Owner:    "client-persist",
		Duration: 5000,
	}
	b, _ := json.Marshal(p)
	req := httptest.NewRequest("POST", "/api/locks/acquire", bytes.NewReader(b))
	rr := httptest.NewRecorder()
	HandleAcquireLock(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("failed to acquire lock: %d", rr.Code)
	}

	// Verify we can get the active lock details
	lockDetails, err := Store.Get("persisted-resource")
	if err != nil || lockDetails == nil {
		t.Fatalf("failed to get lock details: %v", err)
	}

	// Simulating a restart: create a new FileLockStore instance reading the same file
	recoveredStore, err := storage.NewFileLockStore(tempFile)
	if err != nil {
		t.Fatalf("failed to restore from file: %v", err)
	}

	recoveredLock, err := recoveredStore.Get("persisted-resource")
	if err != nil || recoveredLock == nil {
		t.Fatalf("failed to retrieve persisted lock after simulated restart: %v", err)
	}

	if recoveredLock.Owner != "client-persist" || recoveredLock.FencingToken != lockDetails.FencingToken {
		t.Errorf("recovered lock details mismatch: expected owner client-persist and token %d, got owner %q and token %d",
			lockDetails.FencingToken, recoveredLock.Owner, recoveredLock.FencingToken)
	}
}

func TestAPIKeyMiddleware(t *testing.T) {
	apiKey := "my-secret-key"
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				authHeader := r.Header.Get("Authorization")
				if strings.HasPrefix(authHeader, "Bearer ") {
					key = strings.TrimPrefix(authHeader, "Bearer ")
				}
			}
			if key != apiKey {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Test unauthorized request
	req1 := httptest.NewRequest("GET", "/api/locks/observability", nil)
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", rr1.Code)
	}

	// 2. Test authorized request via header
	req2 := httptest.NewRequest("GET", "/api/locks/observability", nil)
	req2.Header.Set("X-API-Key", apiKey)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rr2.Code)
	}

	// 3. Test authorized request via Bearer token
	req3 := httptest.NewRequest("GET", "/api/locks/observability", nil)
	req3.Header.Set("Authorization", "Bearer "+apiKey)
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rr3.Code)
	}
}

func TestLockEventSSE(t *testing.T) {
	Store = storage.NewInMemoryStore()

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest("GET", "/api/locks/subscribe", nil).WithContext(reqCtx)
	rr := httptest.NewRecorder()

	go func() {
		HandleLockSubscribe(rr, req)
	}()

	time.Sleep(50 * time.Millisecond)

	Store.Acquire("sse-resource", "client-sse", "session-sse", 5*time.Second)
	Store.Release("sse-resource", "client-sse", 0)

	time.Sleep(50 * time.Millisecond)
	cancel()

	body := rr.Body.String()
	if !strings.Contains(body, "released") || !strings.Contains(body, "sse-resource") {
		t.Errorf("expected SSE body to contain sse-resource release action, got: %s", body)
	}
}

func TestPriorityWaitQueue(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// Acquire lock by Owner A
	Store.AcquireAdvanced("priority-key", "owner-A", "client-A", 10*time.Second, "exclusive")

	// Waiter B with priority 10
	go func() {
		Store.AcquireAdvancedWithWait("priority-key", "owner-B", "client-B", 10*time.Second, 5*time.Second, 10, "exclusive")
	}()

	// Waiter C with priority 50
	go func() {
		Store.AcquireAdvancedWithWait("priority-key", "owner-C", "client-C", 10*time.Second, 5*time.Second, 50, "exclusive")
	}()

	time.Sleep(50 * time.Millisecond)

	// Release lock by Owner A -> next waiter should be Owner C because priority 50 > 10
	Store.Release("priority-key", "owner-A", 0)

	time.Sleep(50 * time.Millisecond)

	lock, err := Store.Get("priority-key")
	if err != nil {
		t.Fatalf("failed to retrieve lock: %v", err)
	}
	if lock.Owner != "owner-C" {
		t.Errorf("expected owner-C with priority 50 to acquire lock, got %q", lock.Owner)
	}
}

func TestSharedLockMode(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// 1. Acquire shared lock by client A
	l1, err := Store.AcquireAdvanced("shared-key", "owner-A", "client-A", 5*time.Second, "shared")
	if err != nil || l1.Mode != "shared" {
		t.Fatalf("failed to acquire shared lock A: %v", err)
	}

	// 2. Acquire shared lock by client B (should be allowed since compatible mode)
	_, err = Store.AcquireAdvanced("shared-key", "owner-B", "client-B", 5*time.Second, "shared")
	if err != nil {
		t.Fatalf("failed to acquire shared lock B: %v", err)
	}

	// 3. Attempt exclusive lock by client C (should conflict and fail)
	_, err = Store.AcquireAdvanced("shared-key", "owner-C", "client-C", 5*time.Second, "exclusive")
	if err == nil {
		t.Error("expected exclusive lock acquisition to fail due to active shared locks")
	}
}

func TestHeartbeatTimeout(t *testing.T) {
	Store = storage.NewInMemoryStore()

	// Acquire lock with Client ID
	_, err := Store.AcquireAdvanced("hb-key", "owner-hb", "client-hb", 10*time.Second, "exclusive")
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Register heartbeat
	Store.PingHeartbeat("client-hb")

	// Trigger a simulated stale timeout check using the ExpireHeartbeatForTest helper
	Store.ExpireHeartbeatForTest("client-hb")

	// Wait for background cleaner tick (cleaner runs every 500ms)
	time.Sleep(800 * time.Millisecond)

	// Verify lock was garbage-collected due to heartbeat failure
	_, err = Store.Get("hb-key")
	if err == nil {
		t.Error("expected lock to be released due to client heartbeat timeout")
	}
}
