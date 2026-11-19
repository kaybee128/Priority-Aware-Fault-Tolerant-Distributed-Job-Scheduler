package com.example.jobscheduler.api;

import com.example.jobscheduler.common.Priority;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * Submit request. Omit {@code runAt} to run now; set it to schedule for later.
 * Omit {@code idempotencyKey} to disable de-duplication.
 */
public record JobRequest(
        @NotBlank @Size(max = 64) String type,
        JsonNode payload,
        Priority priority,
        Instant runAt,
        @Min(1) @Max(10) Integer maxAttempts,
        @Size(max = 128) String idempotencyKey) {
}
