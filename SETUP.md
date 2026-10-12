# Tether: Setup & Getting Started Guide

A production-grade distributed task queue and job scheduler built from scratch in Go.
Features in-memory queues, worker leases, heartbeats, dead-letter queues (DLQ), write-ahead log (WAL) crash recovery, and Prometheus metrics—with zero external message broker dependencies (no Redis, no RabbitMQ, no Kafka).

---

## Prerequisites

- **Go**: 1.25.7 (or 1.22+)
- **Docker & Docker Compose**: (Required for containerized stack and Grafana dashboards)
- **curl** or any HTTP client (Postman, Bruno, etc.)
- **Git**

---

## 1. Quick Start: Bare Metal (Local Go)

Run the broker directly on your local machine using Go.

### Step 1: Build the Project
```bash
go build ./...
```

### Step 2: Run the Broker
```bash
go run ./cmd/broker
```
By default:
- Broker listens on **`http://localhost:8080`**
- Write-Ahead Log is stored in **`tether.wal`** in the working directory
- A built-in worker pool (3 workers) starts in the background to process `email_job` tasks

You can customize runtime behavior using environment variables:
```bash
# Example with custom settings
TETHER_WAL_PATH=tether.wal TETHER_WORKER_POLL_INTERVAL=1s go run ./cmd/broker
```

---

## 2. Trying the HTTP API

While the broker is running, open another terminal to submit and manage jobs.

### Submit a Job
Submit a new job to a queue (e.g. `email`):

**PowerShell (Recommended on Windows):**
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/queues/email/jobs" -ContentType "application/json" -Body '{"name":"welcome-email","task_type":"email","payload":{"to":"user@example.com"},"max_retries":3}'
```

**Bash / Linux / curl:**
```bash
curl -X POST http://localhost:8080/queues/email/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"welcome-email","task_type":"email","payload":{"to":"user@example.com"},"max_retries":3}'
```
**Response:**
```json
{
  "id": "e838933b-8d19-4cb3-a55e-f007e8a93e36",
  "name": "send-welcome-email",
  "status": "pending",
  "task_type": "email",
  "created_at": "2026-10-13T03:30:00Z"
}
```

---

### Poll & Lease a Job (Worker flow)
Workers poll for pending jobs. When a worker requests a job, Tether locks it with a lease:

**PowerShell:**
```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/queues/email/jobs"
```

**curl:**
```bash
curl -X GET http://localhost:8080/queues/email/jobs
```
**Response:**
```json
{
  "id": "e838933b-8d19-4cb3-a55e-f007e8a93e36",
  "name": "welcome-email",
  "status": "in_progress",
  "lease_duration": "30s"
}
```

---

### Send a Heartbeat (Extend Lease)
For long-running tasks, workers can extend the lease before it expires:

**PowerShell:**
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/queues/email/jobs/<jobId>/heartbeat"
```

**curl:**
```bash
curl -X POST http://localhost:8080/queues/email/jobs/<jobId>/heartbeat
```

---

### Mark Job as Completed
When execution succeeds:

**PowerShell:**
```powershell
Invoke-RestMethod -Method Put -Uri "http://localhost:8080/queues/email/jobs/<jobId>" -ContentType "application/json" -Body '{"status":"completed","payload":{"result":"sent"}}'
```

**curl:**
```bash
curl -X PUT http://localhost:8080/queues/email/jobs/<jobId> \
  -H "Content-Type: application/json" \
  -d '{"status":"completed","payload":{"result":"sent"}}'
```

---

### Report a Job Failure
If a job fails, report failure. Tether will automatically calculate backoff, increment retry count, or send to DLQ if `max_retries` is exceeded:

**PowerShell:**
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/queues/email/jobs/<jobId>/fail" -ContentType "application/json" -Body '{"error":"SMTP server timeout"}'
```

**curl:**
```bash
curl -X POST http://localhost:8080/queues/email/jobs/<jobId>/fail \
  -H "Content-Type: application/json" \
  -d '{"error":"SMTP server timeout"}'
```

---

### Inspect Prometheus Metrics
View real-time gauges and counters:

**PowerShell / curl:**
```powershell
curl.exe http://localhost:8080/metrics
```
Key metrics exposed:
- `tether_jobs_pending`: Jobs awaiting execution
- `tether_jobs_leased`: Jobs currently in flight
- `tether_jobs_completed_total`: Cumulative completed jobs
- `tether_jobs_attempt_failure_total`: Failures before retry limit
- `tether_jobs_dead_letter_total`: Jobs sent to the Dead Letter Queue
- `tether_jobs_dlq_size`: Current size of the Dead Letter Queue

---

## 3. Running with Docker Compose & Grafana (Recommended)

This launches the Broker, Prometheus, and Grafana with pre-provisioned dashboards.

### Start the Stack
```bash
docker compose up -d --build
```

### Access Services
- **Tether Broker API**: [http://localhost:8080](http://localhost:8080)
- **Prometheus Dashboard**: [http://localhost:9090](http://localhost:9090)
- **Grafana Live Dashboard**: [http://localhost:3000/d/tether](http://localhost:3000/d/tether)
  *(Anonymous viewer access is enabled—no login required)*

### Stop the Stack
```bash
# Keep volumes (preserves WAL and metrics):
docker compose down

# Or full reset (wipes WAL and metrics data):
docker compose down -v
```

---

## 4. Crash-Recovery & Load Test

Tether includes an automated load generator and demo script to demonstrate crash recovery and WAL replay.

### Using the Automated Demo Script (Linux / macOS / WSL / Git Bash)
```bash
chmod +x demo.sh
./demo.sh
```

### Manual Crash & Recovery Step-by-Step

1. **Start the stack and open Grafana:**
   - Run `docker compose up -d --build`
   - Open [http://localhost:3000/d/tether](http://localhost:3000/d/tether)

2. **Start the load generator in a separate terminal:**
   ```bash
   # Generates 15 jobs/sec with a 20% simulated failure rate
   go run ./cmd/loadgen -rate 15 -failrate 0.2 -broker http://localhost:8080
   ```
   *Notice the `pending` queue climb and `leased` gauge reflect active workers.*

3. **Simulate a Broker Crash:**
   ```bash
   docker compose stop broker
   ```
   *In Grafana, observe the metrics stop updating (flatline).*

4. **Restart the Broker (WAL Replay):**
   ```bash
   docker compose start broker
   ```
   *Watch Grafana snap back:*
   - `leased` resets to 0 (ephemeral state).
   - `pending` absorbs any uncompleted jobs (no jobs are lost!).
   - `dlq_size` holds durable membership.
   - Counters reset to 0 (in-memory rate metrics).

---

## 5. Kubernetes Deployment (Optional)

Deploy Tether with a StatefulSet and persistent storage on Kubernetes:

```bash
# Deploy broker, headless service, Prometheus, and Grafana
kubectl apply -f k8s/

# Verify status
kubectl get statefulset,pods,svc
```

To clean up:
```bash
kubectl delete -f k8s/
```

---

## 6. Running Tests & Benchmarks

### Unit & Integration Tests
```bash
go test ./...
```

### Run Benchmarks
Test Write-Ahead Log append performance and queue throughput:
```bash
# Benchmark WAL appends and batching
go test -bench=. ./internal/wal

# Benchmark in-memory queue operations
go test -bench=. ./internal/broker
```
