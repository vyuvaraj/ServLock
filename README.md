# ServLock — Distributed Lock Manager

`ServLock` is a high-performance distributed locking manager for the Servverse ecosystem, providing cross-service mutual exclusion with lease-based locks, fencing tokens, reentrant locking, deadlock cycle detection, and metrics observability.

## Features

- **Lease-based Locks**: Automatic expiration of locks to prevent permanent resource hangs.
- **Reentrant Locks**: Reentrant support via `client_id` tracking (recursive acquisition).
- **Fencing Tokens**: Monotonically increasing tokens to prevent stale writes/updates in concurrency.
- **Deadlock Cycle Detection**: Active graph cycle detection aborts cyclic lock wait queues with error status.
- **Observability Metrics**: Prometheus-compatible metric exporter endpoint.
- **Lease Persistence**: Crash-safe persistent lease locking via local JSON file-backing.

---

## Getting Started

### Prerequisites

- Go 1.20+ installed

### Running locally

```bash
# Start in-memory mode on default port 8089
go run main.go

# Start on custom port
go run main.go --port 8090
```

---

## API Specification

All endpoints support standard auth and tenant isolation headers.

### 1. Acquire Lock
Acquires a lock for a key. Blocks up to `wait_ms` if held, and supports reentrancy if matching `client_id` is supplied.

* **Endpoint**: `POST /api/locks/acquire`
* **Request Payload**:
  ```json
  {
    "key": "payment-order-123",
    "owner": "worker-node-1",
    "client_id": "session-abc",
    "duration_ms": 30000,
    "wait_ms": 5000
  }
  ```
* **Response (200 OK)**:
  ```json
  {
    "status": "success",
    "lock": {
      "key": "payment-order-123",
      "owner": "worker-node-1",
      "client_id": "session-abc",
      "reentrancy_count": 1,
      "fencing_token": 15,
      "expires_at": "2026-07-17T20:25:00Z"
    }
  }
  ```

---

### 2. Renew Lock Lease
Extends active lease TTL. Rejects request if the provided fencing token does not match the active lock lease.

* **Endpoint**: `POST /api/locks/renew`
* **Request Payload**:
  ```json
  {
    "key": "payment-order-123",
    "owner": "worker-node-1",
    "fencing_token": 15,
    "duration_ms": 30000
  }
  ```

---

### 3. Release Lock
Frees the lock immediately. If reentrancy count is greater than 1, decrements count instead.

* **Endpoint**: `POST /api/locks/release`
* **Request Payload**:
  ```json
  {
    "key": "payment-order-123",
    "owner": "worker-node-1",
    "fencing_token": 15
  }
  ```

---

### 4. Observability & Metrics

#### List Active Locks
Retrieves list of active leases along with queued waiters.
* **Endpoint**: `GET /api/locks/observability`

#### Prometheus Metrics
Retrieves Prometheus gauges/counters.
* **Endpoint**: `GET /api/locks/metrics`

---

## License

This project is licensed under the GNU Affero General Public License v3 (AGPL-3.0) - see the [LICENSE](LICENSE) file for details.
