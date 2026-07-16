package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"servlock/pkg/storage"

	"github.com/vyuvaraj/ServShared"
)

type LockRequest struct {
	Key      string `json:"key"`
	Owner    string `json:"owner"`
	Duration int    `json:"duration_ms"` // Lease TTL in milliseconds
	WaitTime int    `json:"wait_ms"`     // Optional block/wait timeout in milliseconds
}

type LockResponse struct {
	Status       string        `json:"status"`
	Lock         *storage.Lock `json:"lock,omitempty"`
	Message      string        `json:"message,omitempty"`
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
		lock, err = Store.AcquireWithWait(req.Key, req.Owner, ttl, waitTimeout)
	} else {
		lock, err = Store.Acquire(req.Key, req.Owner, ttl)
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

	released, err := Store.Release(req.Key, req.Owner)
	if err != nil {
		httpError(w, r, err.Error(), http.StatusBadRequest)
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

	ttl := 10 * time.Second
	if req.Duration > 0 {
		ttl = time.Duration(req.Duration) * time.Millisecond
	}

	renewed, err := Store.Renew(req.Key, req.Owner, ttl)
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

func httpError(w http.ResponseWriter, r *http.Request, msg string, status int) {
	var errorCode string
	switch status {
	case http.StatusMethodNotAllowed:
		errorCode = "ERR_METHOD_NOT_ALLOWED"
	case http.StatusBadRequest:
		errorCode = "ERR_BAD_REQUEST"
	case http.StatusUnauthorized:
		errorCode = "ERR_UNAUTHORIZED"
	case http.StatusForbidden:
		errorCode = "ERR_FORBIDDEN"
	case http.StatusNotFound:
		errorCode = "ERR_NOT_FOUND"
	case http.StatusConflict:
		errorCode = "ERR_CONFLICT"
	case http.StatusNotImplemented:
		errorCode = "ERR_NOT_IMPLEMENTED"
	default:
		errorCode = "ERR_INTERNAL_SERVER_ERROR"
	}
	ServShared.WriteJSONError(w, r, msg, errorCode, status)
}

func HandleLockObservability(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, r, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ims, ok := Store.(*storage.InMemoryStore)
	if !ok {
		httpError(w, r, "Observability is only supported for InMemoryStore", http.StatusNotImplemented)
		return
	}

	locks := ims.GetActiveLocks()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(locks)
}
