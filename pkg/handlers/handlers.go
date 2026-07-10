package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"servlock/pkg/storage"
)

type LockRequest struct {
	Key      string `json:"key"`
	Owner    string `json:"owner"`
	Duration int    `json:"duration_ms"` // Lease TTL in milliseconds
}

type LockResponse struct {
	Status       string        `json:"status"`
	Lock         *storage.Lock `json:"lock,omitempty"`
	Message      string        `json:"message,omitempty"`
}

var Store storage.LockBackend

func HandleAcquireLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	if req.Key == "" || req.Owner == "" {
		http.Error(w, "Key and Owner are required fields", http.StatusBadRequest)
		return
	}

	ttl := 10 * time.Second
	if req.Duration > 0 {
		ttl = time.Duration(req.Duration) * time.Millisecond
	}

	lock, err := Store.Acquire(req.Key, req.Owner, ttl)
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
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	released, err := Store.Release(req.Key, req.Owner)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
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
