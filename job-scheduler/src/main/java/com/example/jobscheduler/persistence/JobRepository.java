package com.example.jobscheduler.persistence;

import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.common.JobStatus;
import com.example.jobscheduler.common.Priority;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * All SQL lives here. Every state change is a single conditional UPDATE, so
 * concurrent workers, the reaper, and retries cannot overwrite each other:
 * the WHERE clause encodes which state and which lease owner are allowed.
 */
@Repository
public class JobRepository {

    public record OutboxRow(long id, String topic, String key, String value) {
    }

    private static final RowMapper<Job> JOB_ROW = (rs, rowNum) -> new Job(
            rs.getObject("id", UUID.class),
            rs.getString("idempotency_key"),
            rs.getString("type"),
            rs.getString("payload"),
            Priority.valueOf(rs.getString("priority")),
            JobStatus.valueOf(rs.getString("status")),
            instant(rs.getTimestamp("run_at")),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getString("lease_owner"),
            rs.getString("last_error"),
            instant(rs.getTimestamp("created_at")),
            instant(rs.getTimestamp("updated_at")),
            instant(rs.getTimestamp("completed_at")));

    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ---------- reads ----------

    public Optional<Job> findById(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = ?", JOB_ROW, id).stream().findFirst();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT * FROM jobs WHERE idempotency_key = ?", JOB_ROW, key).stream().findFirst();
    }

    public Map<String, Long> countByStatus() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (Map<String, Object> row : jdbc.queryForList("SELECT status, count(*) AS c FROM jobs GROUP BY status")) {
            out.put((String) row.get("status"), ((Number) row.get("c")).longValue());
        }
        return out;
    }

    /** RUNNING jobs whose last claim is older than {@code before}; candidates for lease checks. */
    public List<Job> findStaleRunning(Instant before, int limit) {
        return jdbc.query(
                "SELECT * FROM jobs WHERE status = 'RUNNING' AND updated_at < ? ORDER BY updated_at LIMIT ?",
                JOB_ROW, ts(before), limit);
    }

    // ---------- submission ----------

    /** @return false if a job with the same idempotency key already exists (nothing inserted). */
    public boolean insertJob(UUID id, String idempotencyKey, String type, String payload,
                             Priority priority, JobStatus status, Instant runAt, int maxAttempts) {
        return jdbc.update("""
                INSERT INTO jobs (id, idempotency_key, type, payload, priority, status, run_at, max_attempts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                id, idempotencyKey, type, payload, priority.name(), status.name(), ts(runAt), maxAttempts) == 1;
    }

    // ---------- outbox ----------

    public void insertOutbox(String topic, UUID jobId) {
        jdbc.update("INSERT INTO outbox (topic, message_key, message_value) VALUES (?, ?, ?)",
                topic, jobId.toString(), jobId.toString());
    }

    /** Locks up to {@code limit} unpublished outbox rows for the current transaction. */
    public List<OutboxRow> lockUnpublished(int limit) {
        return jdbc.query("""
                SELECT id, topic, message_key, message_value FROM outbox
                WHERE published_at IS NULL ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED
                """,
                (rs, rowNum) -> new OutboxRow(rs.getLong("id"), rs.getString("topic"),
                        rs.getString("message_key"), rs.getString("message_value")),
                limit);
    }

    public void markPublished(long outboxId) {
        jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", outboxId);
    }

    // ---------- dispatch: SCHEDULED (due) -> QUEUED + outbox message ----------

    /** Moves due jobs to QUEUED and enqueues them in one statement. Safe with many API replicas. */
    public int dispatchDue(int limit) {
        return jdbc.update("""
                WITH due AS (
                    SELECT id FROM jobs
                    WHERE status = 'SCHEDULED' AND run_at <= now()
                    ORDER BY run_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                ), moved AS (
                    UPDATE jobs j SET status = 'QUEUED', updated_at = now()
                    FROM due WHERE j.id = due.id
                    RETURNING j.id, j.priority
                )
                INSERT INTO outbox (topic, message_key, message_value)
                SELECT 'jobs.' || lower(priority), id::text, id::text FROM moved
                """, limit);
    }

    // ---------- worker transitions ----------

    /** QUEUED -> RUNNING. Returns the claimed job, or empty if someone else already claimed it. */
    public Optional<Job> claim(UUID id, String workerId) {
        return jdbc.query("""
                UPDATE jobs SET status = 'RUNNING', attempts = attempts + 1,
                                lease_owner = ?, updated_at = now()
                WHERE id = ? AND status = 'QUEUED'
                RETURNING *
                """, JOB_ROW, workerId, id).stream().findFirst();
    }

    public boolean markSucceeded(UUID id, String owner) {
        return jdbc.update("""
                UPDATE jobs SET status = 'SUCCEEDED', completed_at = now(), updated_at = now(),
                                lease_owner = NULL, last_error = NULL
                WHERE id = ? AND status = 'RUNNING' AND lease_owner = ?
                """, id, owner) == 1;
    }

    public boolean retry(UUID id, String owner, Instant runAt, String error) {
        return jdbc.update("""
                UPDATE jobs SET status = 'SCHEDULED', run_at = ?, last_error = ?,
                                lease_owner = NULL, updated_at = now()
                WHERE id = ? AND status = 'RUNNING' AND lease_owner = ?
                """, ts(runAt), error, id, owner) == 1;
    }

    public boolean dead(UUID id, String owner, String error) {
        return jdbc.update("""
                UPDATE jobs SET status = 'DEAD', completed_at = now(), updated_at = now(),
                                lease_owner = NULL, last_error = ?
                WHERE id = ? AND status = 'RUNNING' AND lease_owner = ?
                """, error, id, owner) == 1;
    }

    // ---------- helpers ----------

    static Instant instant(Timestamp t) {
        return t == null ? null : t.toInstant();
    }

    static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }
}
