package com.example.jobscheduler.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Exponential retry delay: base * 2^(attempt-1), capped at max. */
@Component
public class Backoff {

    private final long baseMs;
    private final long maxMs;

    public Backoff(@Value("${scheduler.backoff.base-ms:2000}") long baseMs,
                   @Value("${scheduler.backoff.max-ms:60000}") long maxMs) {
        this.baseMs = baseMs;
        this.maxMs = maxMs;
    }

    /** @param attempt the attempt that just failed (1-based) */
    public Duration delayAfter(int attempt) {
        long factor = 1L << Math.min(Math.max(attempt - 1, 0), 20);
        return Duration.ofMillis(Math.min(baseMs * factor, maxMs));
    }
}
