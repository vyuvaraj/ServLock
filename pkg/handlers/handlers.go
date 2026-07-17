package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"servlock/pkg/storage"
)

type LockRequest struct {
	Key          string `json:"key"`
	Owner        string `json:"owner"`
	ClientID     string `json:"client_id"`
	FencingToken int64  `json:"fencing_token"`
	Duration     int    `json:"duration_ms"` // Lease TTL in milliseconds
	WaitTime     int    `json:"wait_ms"`     // Optional block/wait timeout in milliseconds
	Mode         string `json:"mode"`        // "shared" or "exclusive"
	Priority     int    `json:"priority"`
}

type LockResponse struct {
	Status  string        `json:"status"`
	Lock    *storage.Lock `json:"lock,omitempty"`
	Message string        `json:"message,omitempty"`
}

var Store storage.LockBackend

func HandleAcquireLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, r, "Invalid payload", http.StatusBadRequest)
		return
	}

	if req.Key == "" || req.Owner == "" {
		httpError(w, r, "Key and Owner are required fields", http.StatusBadRequest)
		return
	}

	ttl := 10 * time.Second
	if req.Duration > 0 {
		ttl = time.Duration(req.Duration) * time.Millisecond
	}

	var lock *storage.Lock
	var err error
	if req.WaitTime > 0 {
		waitTimeout := time.Duration(req.WaitTime) * time.Millisecond
		lock, err = Store.AcquireAdvancedWithWait(req.Key, req.Owner, req.ClientID, ttl, waitTimeout, req.Priority, req.Mode)
	} else {
		lock, err = Store.AcquireAdvanced(req.Key, req.Owner, req.ClientID, ttl, req.Mode)
	}

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "failed",
			Message: err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(LockResponse{
		Status: "success",
		Lock:   lock,
	})
}

func HandleReleaseLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, r, "Invalid payload", http.StatusBadRequest)
		return
	}

	if req.Key == "" || req.Owner == "" {
		httpError(w, r, "Key and Owner are required fields", http.StatusBadRequest)
		return
	}

	released, err := Store.Release(req.Key, req.Owner, req.FencingToken)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "failed",
			Message: err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if released {
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "success",
			Message: "Lock released successfully",
		})
	} else {
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "failed",
			Message: "Lock was not active or already expired",
		})
	}
}

func HandleRenewLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, r, "Invalid payload", http.StatusBadRequest)
		return
	}

	if req.Key == "" || req.Owner == "" {
		httpError(w, r, "Key and Owner are required fields", http.StatusBadRequest)
		return
	}

	ttl := 10 * time.Second
	if req.Duration > 0 {
		ttl = time.Duration(req.Duration) * time.Millisecond
	}

	renewed, err := Store.Renew(req.Key, req.Owner, req.FencingToken, ttl)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "failed",
			Message: err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if renewed {
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "success",
			Message: "Lock lease renewed successfully",
		})
	} else {
		json.NewEncoder(w).Encode(LockResponse{
			Status:  "failed",
			Message: "Lock lease could not be renewed",
		})
	}
}

func HandleLockObservability(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ims, ok := Store.(*storage.InMemoryStore)
	if !ok {
		// If using file-backed store, we cast its embedded InMemoryStore
		if fbs, isFile := Store.(*storage.FileLockStore); isFile {
			ims = fbs.InMemoryStore
		}
	}

	if ims == nil {
		httpError(w, r, "Observability is only supported for InMemory or File stores", http.StatusNotImplemented)
		return
	}

	locks := ims.GetActiveLocks()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(locks)
}

func httpError(w http.ResponseWriter, r *http.Request, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(LockResponse{
		Status:  "failed",
		Message: msg,
	})
}

func HandleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ims, ok := Store.(*storage.InMemoryStore)
	if !ok {
		if fbs, isFile := Store.(*storage.FileLockStore); isFile {
			ims = fbs.InMemoryStore
		}
	}

	if ims == nil {
		httpError(w, r, "Metrics are only supported for InMemoryStore", http.StatusNotImplemented)
		return
	}

	locks := ims.GetActiveLocks()
	activeCount := len(locks)
	waitersCount := 0
	for _, l := range locks {
		waitersCount += len(l.Waiters)
	}
	deadlockCount := ims.GetDeadlockCount()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "# HELP servlock_active_locks Number of active locks currently held\n")
	fmt.Fprintf(w, "# TYPE servlock_active_locks gauge\n")
	fmt.Fprintf(w, "servlock_active_locks %d\n\n", activeCount)

	fmt.Fprintf(w, "# HELP servlock_waiters_count Total number of clients waiting for locks\n")
	fmt.Fprintf(w, "# TYPE servlock_waiters_count gauge\n")
	fmt.Fprintf(w, "servlock_waiters_count %d\n\n", waitersCount)

	fmt.Fprintf(w, "# HELP servlock_deadlocks_total Total number of deadlocks detected\n")
	fmt.Fprintf(w, "# TYPE servlock_deadlocks_total counter\n")
	fmt.Fprintf(w, "servlock_deadlocks_total %d\n", deadlockCount)
}

func HandleLockSubscribe(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := Store.Subscribe()
	defer Store.Unsubscribe(ch)

	fmt.Fprintf(w, ": keep-alive\n\n")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case event := <-ch:
			data, err := json.Marshal(event)
			if err == nil {
				fmt.Fprintf(w, "data: %s\n\n", string(data))
				flusher.Flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

type HeartbeatRequest struct {
	ClientID string `json:"client_id"`
}

func HandleHeartbeatPing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, r, "Invalid payload", http.StatusBadRequest)
		return
	}
	if req.ClientID == "" {
		httpError(w, r, "client_id is required", http.StatusBadRequest)
		return
	}
	Store.PingHeartbeat(req.ClientID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}
