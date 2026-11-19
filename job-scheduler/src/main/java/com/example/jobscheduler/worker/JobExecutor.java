package com.example.jobscheduler.worker;

import com.example.jobscheduler.common.Job;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Executes job types. Add real handlers here; the scheduler itself is type-agnostic.
 *
 *  - "echo":  no-op that succeeds (smoke tests)
 *  - "sleep": simulated work. payload {"durationMs": 200, "failureRate": 0.0}
 *             Used by the evaluation harness.
 */
@Component
public class JobExecutor {

    private static final long MAX_SLEEP_MS = 600_000;

    private final ObjectMapper mapper;

    public JobExecutor(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** @throws Exception any exception means the attempt failed and will be retried. */
    public void execute(Job job) throws Exception {
        switch (job.type()) {
            case "echo" -> {
                // nothing to do
            }
            case "sleep" -> simulate(job.payload());
            default -> throw new IllegalArgumentException("unknown job type: " + job.type());
        }
    }

    private void simulate(String payload) throws Exception {
        JsonNode p = mapper.readTree(payload);
        long durationMs = Math.min(Math.max(p.path("durationMs").asLong(100), 0), MAX_SLEEP_MS);
        double failureRate = p.path("failureRate").asDouble(0.0);
        Thread.sleep(durationMs);
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new IllegalStateException("simulated failure");
        }
    }
}
