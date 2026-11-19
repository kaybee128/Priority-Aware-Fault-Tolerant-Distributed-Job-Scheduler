package com.example.jobscheduler.common;

import java.time.Instant;
import java.util.UUID;

/** Immutable snapshot of a row in the {@code jobs} table. */
public record Job(
        UUID id,
        String idempotencyKey,
        String type,
        String payload,
        Priority priority,
        JobStatus status,
        Instant runAt,
        int attempts,
        int maxAttempts,
        String leaseOwner,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {
}
