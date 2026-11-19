package com.example.jobscheduler.api;

import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.common.JobStatus;
import com.example.jobscheduler.common.Priority;

import java.time.Instant;
import java.util.UUID;

/** Public representation of a job. Payload and lease internals are not exposed. */
public record JobView(
        UUID id,
        String type,
        Priority priority,
        JobStatus status,
        int attempts,
        int maxAttempts,
        Instant runAt,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt,
        String lastError) {

    public static JobView from(Job j) {
        return new JobView(j.id(), j.type(), j.priority(), j.status(), j.attempts(), j.maxAttempts(),
                j.runAt(), j.createdAt(), j.updatedAt(), j.completedAt(), j.lastError());
    }
}
