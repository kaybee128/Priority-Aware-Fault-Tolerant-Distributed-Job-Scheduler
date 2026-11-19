package com.example.jobscheduler.service;

import com.example.jobscheduler.common.Backoff;
import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.common.Topics;
import com.example.jobscheduler.persistence.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Every job state transition used by workers and the reaper, each in its own transaction.
 */
@Service
public class JobStateService {

    public enum Outcome {
        /** Job is SCHEDULED again; the dispatcher will re-queue it. */
        RETRY_SCHEDULED,
        /** No attempts left; job is DEAD and a copy was written to the DLQ topic. */
        DEAD,
        /** The lease owner changed or the job already moved on; nothing was changed. */
        LEASE_LOST
    }

    private final JobRepository repo;
    private final Backoff backoff;

    public JobStateService(JobRepository repo, Backoff backoff) {
        this.repo = repo;
        this.backoff = backoff;
    }

    @Transactional
    public Optional<Job> claim(UUID id, String workerId) {
        return repo.claim(id, workerId);
    }

    @Transactional
    public boolean succeed(UUID id, String owner) {
        return repo.markSucceeded(id, owner);
    }

    /** Called by a worker whose execution threw. */
    @Transactional
    public Outcome fail(Job job, String owner, String error) {
        if (job.attempts() >= job.maxAttempts()) {
            return markDead(job, owner, error);
        }
        Instant retryAt = Instant.now().plus(backoff.delayAfter(job.attempts()));
        return repo.retry(job.id(), owner, retryAt, error) ? Outcome.RETRY_SCHEDULED : Outcome.LEASE_LOST;
    }

    /** Called by the reaper for a RUNNING job whose Redis lease has disappeared. */
    @Transactional
    public Outcome expire(Job job) {
        String owner = job.leaseOwner();
        if (owner == null) {
            return Outcome.LEASE_LOST;
        }
        String reason = "lease expired; worker " + owner + " presumed dead";
        if (job.attempts() >= job.maxAttempts()) {
            return markDead(job, owner, reason);
        }
        // Retry immediately: run_at = now, so the dispatcher re-queues it on its next tick.
        return repo.retry(job.id(), owner, Instant.now(), reason) ? Outcome.RETRY_SCHEDULED : Outcome.LEASE_LOST;
    }

    private Outcome markDead(Job job, String owner, String error) {
        if (!repo.dead(job.id(), owner, error)) {
            return Outcome.LEASE_LOST;
        }
        repo.insertOutbox(Topics.DLQ, job.id());
        return Outcome.DEAD;
    }
}
