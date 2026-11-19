package com.example.jobscheduler.worker;

import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.common.JobStatus;
import com.example.jobscheduler.common.Priority;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobExecutorTest {

    private final JobExecutor executor = new JobExecutor(new ObjectMapper());

    @Test
    void echoSucceeds() {
        assertThatCode(() -> executor.execute(job("echo", "{}"))).doesNotThrowAnyException();
    }

    @Test
    void sleepWithFailureRateOneAlwaysFails() {
        assertThatThrownBy(() -> executor.execute(job("sleep", "{\"durationMs\":1,\"failureRate\":1.0}")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sleepWithFailureRateZeroAlwaysSucceeds() {
        assertThatCode(() -> executor.execute(job("sleep", "{\"durationMs\":1,\"failureRate\":0.0}")))
                .doesNotThrowAnyException();
    }

    @Test
    void unknownTypeFails() {
        assertThatThrownBy(() -> executor.execute(job("nope", "{}")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Job job(String type, String payload) {
        Instant now = Instant.now();
        return new Job(UUID.randomUUID(), null, type, payload, Priority.NORMAL, JobStatus.RUNNING,
                now, 1, 3, "test-worker", null, now, now, null);
    }
}
