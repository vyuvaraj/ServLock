# ServLock — Distributed Lock Manager

`ServLock` is a distributed locking manager of the Servverse ecosystem, providing cross-service mutual exclusion with lease-based locks, fencing tokens, and renewal capabilities.

## Getting Started

### Prerequisites

- Go 1.20+ installed

### Running locally

```bash
# Start on default port 8089
go run main.go

# Start on custom port
go run main.go --port 8090
```

## API Specification

### 1. Acquire Lock
Acquires a lock for a key. Returns HTTP 409 Conflict if key is already locked by another owner.

`POST /api/locks/acquire`

```json
{
  "key": "payment-order-123",
  "owner": "worker-node-1",
  "duration_ms": 30000
}
```

Response:
```json
{
  "status": "success",
  "lock": {
    "key": "payment-order-123",
    "owner": "worker-node-1",
    "fencing_token": 1,
    "expires_at": "2026-07-10T20:25:00Z"
  }
}
```

### 2. Renew Lock Lease
Extends active lease TTL.

`POST /api/locks/renew`

```json
{
  "key": "payment-order-123",
  "owner": "worker-node-1",
  "duration_ms": 30000
}
```

### 3. Release Lock
Frees the lock immediately.

`POST /api/locks/release`

```json
{
  "key": "payment-order-123",
  "owner": "worker-node-1"
}
```
