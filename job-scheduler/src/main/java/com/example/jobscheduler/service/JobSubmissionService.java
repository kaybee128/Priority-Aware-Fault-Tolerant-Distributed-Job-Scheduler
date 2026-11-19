package com.example.jobscheduler.service;

import com.example.jobscheduler.api.JobRequest;
import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.common.JobStatus;
import com.example.jobscheduler.common.Priority;
import com.example.jobscheduler.persistence.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobSubmissionService {

    /** @param created false when an existing job was returned for the same idempotency key */
    public record Result(Job job, boolean created) {
    }

    private static final int DEFAULT_MAX_ATTEMPTS = 3;

    private final JobRepository repo;

    public JobSubmissionService(JobRepository repo) {
        this.repo = repo;
    }

    /**
     * Inserts the job and, if it is due now, its outbox message in the same transaction.
     * Future jobs stay SCHEDULED until the dispatcher moves them.
     */
    @Transactional
    public Result submit(JobRequest req) {
        if (req.idempotencyKey() != null) {
            Optional<Job> existing = repo.findByIdempotencyKey(req.idempotencyKey());
            if (existing.isPresent()) {
                return new Result(existing.get(), false);
            }
        }

        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        Instant runAt = req.runAt() == null ? now : req.runAt();
        boolean due = !runAt.isAfter(now);
        Priority priority = req.priority() == null ? Priority.NORMAL : req.priority();
        int maxAttempts = req.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : req.maxAttempts();
        String payload = req.payload() == null ? "{}" : req.payload().toString();

        boolean inserted = repo.insertJob(id, req.idempotencyKey(), req.type(), payload, priority,
                due ? JobStatus.QUEUED : JobStatus.SCHEDULED, runAt, maxAttempts);
        if (!inserted) {
            // Lost a race with another request using the same idempotency key.
            Job winner = repo.findByIdempotencyKey(req.idempotencyKey()).orElseThrow();
            return new Result(winner, false);
        }
        if (due) {
            repo.insertOutbox(priority.topic(), id);
        }
        return new Result(repo.findById(id).orElseThrow(), true);
    }
}
