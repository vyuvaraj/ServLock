package handlers

import (
	"bytes"
	"encoding/json"
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
