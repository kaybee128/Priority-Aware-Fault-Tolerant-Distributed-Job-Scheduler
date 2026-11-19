# Distributed Job Scheduler

A fault-tolerant job scheduler built with Spring Boot, Kafka, Redis, and PostgreSQL.
Jobs survive worker crashes: a dead worker's lease expires and the job is retried
on another worker. An evaluation harness measures correctness and latency under
load and worker kills.

## Architecture

```
            ┌──────────── api profile ─────────────┐
Client ───► │ REST API ──► PostgreSQL (jobs+outbox)│
            │ Outbox publisher ─┐  Dispatcher       │
            │ Reaper (failure detection/recovery)   │
            └───────────────────┼──────────────────┘
                                ▼
                 Kafka: jobs.high | jobs.normal | jobs.low | jobs.dlq
                                │  (consumer group "job-workers")
            ┌──────────── worker profile ─────────────┐
            │ Priority-lane listeners → claim (PG)     │
            │  → lease (Redis) → execute → succeed/fail│
            │  heartbeat renews lease while running    │
            └──────────────────────────────────────────┘
```

**Job lifecycle**

```
SCHEDULED ──(run_at due, dispatcher)──► QUEUED ──(worker claims)──► RUNNING
RUNNING ──success──────────────────────────────────────────────────► SUCCEEDED
RUNNING ──failure, attempts left──► SCHEDULED (backoff) ──► … back to QUEUED
RUNNING ──failure, no attempts left──► DEAD (copy on jobs.dlq)
RUNNING ──worker died, lease expired (reaper)──► SCHEDULED (run_at = now) ──► …
```

## Design decisions

| Decision | Why |
|---|---|
| PostgreSQL is the source of truth; Redis holds only leases | Redis is not durable. Job state must survive a Redis restart. |
| Every state change is one conditional `UPDATE` (state + lease owner in the `WHERE`) | Two workers, or a worker and the reaper, cannot both win. A slow worker that lost its lease cannot overwrite the new owner's result. |
| Transactional outbox for Kafka publishing | The job row and its queue message commit together. No job is stranded without a message, and none is queued without a job. |
| At-least-once delivery | A crash after a job runs but before its result is saved means it runs again. Jobs must be idempotent. The `attempts` counter shows when this happened. |
| Priority = separate topics plus weighted listener threads | Kafka has no native priority queue. High lanes get more threads. Running jobs are never preempted. |
| Leases in Redis with heartbeats; reaper checks for missing keys | Failure detection is bounded by the lease TTL (default 10 s). |
| Retries with exponential backoff, then DLQ | Transient failures recover. Permanent failures stop consuming capacity and are kept for inspection. |
| One artifact, two roles chosen by Spring profile (`api`, `worker`) | Simple to build. The roles scale independently in Compose or Kubernetes. |

**Known limitations (be upfront about these in interviews)**

- Priority is weighted, not strict. Under saturation a LOW job waits for a free LOW thread, not for every HIGH job.
- Recovery is at-least-once. A job whose worker died after finishing the work but before committing will run twice.
- A job running longer than `max.poll.interval` (default 5 min) will trigger a Kafka rebalance. Keep jobs shorter, or raise that setting.
- No cron schedules yet; `runAt` one-shot scheduling only.
- The outbox table is never pruned. Add a cleanup job before running for long periods.
- No authentication on the API.

## Status

| Area | State |
|---|---|
| Maven build (`mvn compile`, `mvn test-compile`) | **Verified**: compiles with Java 25 targeting release 21 |
| Unit tests (`BackoffTest`, `JobExecutorTest`) | **Verified**: 5 of 5 pass |
| Flyway schema (`V1__init.sql`) | Written, **not yet run** against Postgres |
| Integration test (`EndToEndTest`, Testcontainers) | Written, **not yet run**; skipped automatically without Docker |
| Docker image and `docker-compose.yml` | Written, **not yet run** |
| Evaluation harness (`eval/loadtest.py`) | Syntax-checked; **not yet run** against a live stack |
| Kubernetes manifests, AWS deployment | **Not started** |
| Cron scheduling, auth, outbox pruning | **Not started** |

The first run of the full stack will likely expose small issues in SQL, Kafka, or Redis configuration. Run the steps below and check the output at each stage.

## Run it step by step

### Prerequisites
- Docker Desktop (running)
- Python 3.10+ (only for the evaluation harness)

### 1. Start the stack with 3 workers
```bash
cd job-scheduler
docker compose up -d --build --scale worker=3
```

### 2. Check that everything is up
```bash
docker compose ps
curl -s http://localhost:8080/actuator/health
```
Expected: `api`, `postgres`, `redis`, `kafka` running, and `"status":"UP"` from health.
If `api` or `worker` restarts, check logs: `docker compose logs api`.

### 3. Submit a job
```bash
curl -s -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","priority":"HIGH"}'
```
Copy the `id` from the response, then check its status:
```bash
curl -s http://localhost:8080/api/jobs/<id>
```
It should reach `"status":"SUCCEEDED"` within a second or two.

### 4. Try idempotency, scheduling, and failure
```bash
# Same key twice: second call returns 200 with the same id
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"echo","idempotencyKey":"order-42"}'
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"echo","idempotencyKey":"order-42"}'

# Scheduled for later (ISO-8601 instant)
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d "{\"type\":\"echo\",\"runAt\":\"$(date -u -v+30S +%Y-%m-%dT%H:%M:%SZ)\"}"

# Always fails, dead after 2 attempts (watch attempts go 1 -> 2 -> DEAD)
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"durationMs":10,"failureRate":1.0},"maxAttempts":2}'

# Counts by status
curl -s http://localhost:8080/api/jobs/stats
```

### 5. Watch a worker die (manual recovery demo)
```bash
# Submit slow jobs, then kill one worker mid-run
for i in $(seq 1 30); do
  curl -s -o /dev/null -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
    -d '{"type":"sleep","payload":{"durationMs":5000}}'
done
docker kill $(docker compose ps -q worker | head -1)
docker compose logs -f api   # look for "recovered job ... from dead worker"
```
Jobs owned by the killed worker reappear as `attempts: 2` and still succeed.
Note that the compose file sets no restart policy for workers, so restore capacity with:
```bash
docker compose up -d --no-recreate --scale worker=3
```

### 6. Run the unit tests (no Docker needed)
```bash
docker run --rm -v "$PWD":/src -w /src maven:3.9-eclipse-temurin-21 \
  mvn -B -Dtest='BackoffTest,JobExecutorTest' -Dsurefire.failIfNoSpecifiedTests=false test
```

### 7. Run the full integration test (needs Docker)
```bash
docker run --rm -v "$PWD":/src -w /src \
  -v /var/run/docker.sock:/var/run/docker.sock \
  maven:3.9-eclipse-temurin-21 mvn -B test
```
This starts Postgres, Kafka, and Redis containers through Testcontainers, so it needs the Docker socket.
If you run Maven on the host instead, install Java 21 and Maven, then run `mvn -B test`.

### 8. Run the evaluation
Baseline (no failures):
```bash
python3 eval/loadtest.py --jobs 2000 --kill 0 --out eval/results/baseline
```
With 3 worker SIGKILLs during the run:
```bash
python3 eval/loadtest.py --jobs 2000 --kill 3 --kill-after 5 --kill-gap 8 --out eval/results/kill-3
```
Each run writes `results.json` and `summary.md` into the `--out` folder.
Report what the summary shows, including any jobs unfinished at the timeout. Do not describe the results as better than what the numbers show.

Useful flags: `--mix 0.2,0.6,0.2` (HIGH,NORMAL,LOW), `--sleep-ms 200`, `--timeout 300`, `--seed 42`, `--replicas 3`.

### 9. Tear down
```bash
docker compose down -v
```

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SPRING_PROFILES_ACTIVE` | — | `api` or `worker` |
| `DB_URL`, `DB_USER`, `DB_PASSWORD` | local Postgres | Database |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `REDIS_HOST`, `REDIS_PORT` | `localhost`, `6379` | Redis |
| `LEASE_TTL_MS` | `10000` | Lease length; the failure-detection bound |
| `WORKER_ID` | hostname + random suffix | Identity written to leases and rows |

Tuning values live in `src/main/resources/application.yml`: heartbeat interval, reaper and dispatch intervals, backoff, and lane thread counts.

## Layout

```
src/main/java/com/example/jobscheduler/
  api/         REST controller, request/response records
  common/      Job model, enums, topic names, backoff
  config/      Kafka topic definitions
  control/     Outbox publisher, dispatcher, reaper (api role)
  lease/       Redis lease store (compare-and-set scripts)
  persistence/ All SQL (JdbcTemplate)
  service/     Submission and state transitions, each transactional
  worker/      Kafka priority-lane consumer, executor, heartbeat (worker role)
src/main/resources/db/migration/   Flyway schema
src/test/                          Unit tests and Testcontainers end-to-end test
eval/loadtest.py                   Evaluation harness (stdlib only)
```
