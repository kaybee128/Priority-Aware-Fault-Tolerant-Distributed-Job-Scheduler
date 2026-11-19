package com.example.jobscheduler.worker;

import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.common.Priority;
import com.example.jobscheduler.common.Topics;
import com.example.jobscheduler.lease.LeaseStore;
import com.example.jobscheduler.service.JobStateService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes job ids from the three priority topics and executes them.
 *
 * Each listener thread first waits for an execution slot from {@link ExecutionGate}.
 * Slots are shared across lanes and granted in priority order. Only then does it claim
 * the job, so the job stays QUEUED while it waits.
 *
 * Message flow for one job:
 *   wait for slot -> claim (QUEUED -> RUNNING, Postgres) -> acquire lease (Redis)
 *   -> execute -> succeed | fail (retry or DEAD) -> release lease -> release slot
 * While executing, a heartbeat keeps the lease alive. If the worker dies, the lease
 * expires and the reaper recovers the job.
 */
@Component
@Profile("worker")
public class JobWorker {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private final String workerId;
    private final JobStateService state;
    private final LeaseStore leases;
    private final JobExecutor executor;
    private final ExecutionGate gate;
    private final Counter succeeded;
    private final Counter failedAttempts;
    private final Counter claimsSkipped;
    private final Set<UUID> owned = ConcurrentHashMap.newKeySet();

    public JobWorker(JobStateService state, LeaseStore leases, JobExecutor executor, ExecutionGate gate,
                     MeterRegistry metrics, @Value("${scheduler.worker-id:}") String configuredId) {
        this.workerId = (configuredId == null || configuredId.isBlank()) ? defaultWorkerId() : configuredId;
        this.state = state;
        this.leases = leases;
        this.executor = executor;
        this.gate = gate;
        this.succeeded = metrics.counter("scheduler.jobs.succeeded");
        this.failedAttempts = metrics.counter("scheduler.jobs.failed_attempts");
        this.claimsSkipped = metrics.counter("scheduler.claims.skipped");
    }

    public String workerId() {
        return workerId;
    }

    // Listener thread counts only need to be large enough to keep waiters for every lane.
    // The number of jobs running at once is set by scheduler.worker.slots, not by these.
    @KafkaListener(id = "lane-high", topics = Topics.HIGH, groupId = Topics.CONSUMER_GROUP,
            concurrency = "${scheduler.lanes.high:8}")
    public void onHigh(String jobId) {
        run(jobId, Priority.HIGH);
    }

    @KafkaListener(id = "lane-normal", topics = Topics.NORMAL, groupId = Topics.CONSUMER_GROUP,
            concurrency = "${scheduler.lanes.normal:8}")
    public void onNormal(String jobId) {
        run(jobId, Priority.NORMAL);
    }

    @KafkaListener(id = "lane-low", topics = Topics.LOW, groupId = Topics.CONSUMER_GROUP,
            concurrency = "${scheduler.lanes.low:8}")
    public void onLow(String jobId) {
        run(jobId, Priority.LOW);
    }

    private void run(String jobId, Priority lane) {
        try {
            gate.acquire(lane);
        } catch (InterruptedException e) {
            // Offset is not committed, so the message is redelivered later.
            Thread.currentThread().interrupt();
            return;
        }
        try {
            handle(jobId);
        } finally {
            gate.release();
        }
    }

    void handle(String rawJobId) {
        UUID id = UUID.fromString(rawJobId);

        Optional<Job> claimed = state.claim(id, workerId);
        if (claimed.isEmpty()) {
            // Duplicate message, or the job was already claimed/finished. Safe to ignore.
            claimsSkipped.increment();
            return;
        }
        Job job = claimed.get();
        leases.acquire(id, workerId);
        owned.add(id);
        try {
            executor.execute(job);
            if (state.succeed(id, workerId)) {
                succeeded.increment();
            } else {
                log.warn("job {} finished but its lease was lost; result discarded", id);
            }
        } catch (Exception e) {
            failedAttempts.increment();
            String error = truncate(e.getClass().getSimpleName() + ": " + e.getMessage());
            log.info("job {} attempt {}/{} failed: {}", id, job.attempts(), job.maxAttempts(), error);
            JobStateService.Outcome outcome = state.fail(job, workerId, error);
            log.debug("job {} failure handled as {}", id, outcome);
        } finally {
            owned.remove(id);
            leases.release(id, workerId);
        }
    }

    @Scheduled(fixedDelayString = "${scheduler.heartbeat-interval-ms:3000}")
    public void heartbeat() {
        for (UUID id : owned) {
            if (!leases.renew(id, workerId)) {
                log.warn("lease for job {} is no longer held by {}", id, workerId);
                owned.remove(id);
            }
        }
    }

    private static String defaultWorkerId() {
        String host = System.getenv().getOrDefault("HOSTNAME", "worker");
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static String truncate(String s) {
        return s.length() > 500 ? s.substring(0, 500) : s;
    }
}
