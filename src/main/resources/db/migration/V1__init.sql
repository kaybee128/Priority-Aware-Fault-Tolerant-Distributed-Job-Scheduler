-- Source of truth for every job. Redis holds only short-lived leases.
CREATE TABLE jobs (
    id              UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE,
    type            VARCHAR(64)  NOT NULL,
    payload         TEXT         NOT NULL,
    priority        VARCHAR(16)  NOT NULL CHECK (priority IN ('HIGH', 'NORMAL', 'LOW')),
    status          VARCHAR(16)  NOT NULL CHECK (status IN ('SCHEDULED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'DEAD')),
    run_at          TIMESTAMPTZ  NOT NULL,
    attempts        INT          NOT NULL DEFAULT 0,
    max_attempts    INT          NOT NULL,
    lease_owner     VARCHAR(128),
    last_error      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

-- Dispatcher scans due SCHEDULED jobs; reaper scans RUNNING jobs by claim time.
CREATE INDEX idx_jobs_due     ON jobs (run_at)     WHERE status = 'SCHEDULED';
CREATE INDEX idx_jobs_running ON jobs (updated_at) WHERE status = 'RUNNING';

-- Transactional outbox: rows are written in the same transaction as the job
-- state change and published to Kafka by a poller (at-least-once).
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    topic         VARCHAR(64) NOT NULL,
    message_key   VARCHAR(64) NOT NULL,
    message_value VARCHAR(64) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending ON outbox (id) WHERE published_at IS NULL;
