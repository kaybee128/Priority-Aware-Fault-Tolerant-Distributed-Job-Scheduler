package com.example.jobscheduler.control;

import com.example.jobscheduler.common.Job;
import com.example.jobscheduler.lease.LeaseStore;
import com.example.jobscheduler.persistence.JobRepository;
import com.example.jobscheduler.service.JobStateService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Failure detection and recovery.
 *
 * A RUNNING job is considered orphaned when its Redis lease key is gone. Leases expire
 * after {@code lease-ttl-ms} without a heartbeat, so a crashed worker is detected within
 * roughly one TTL plus one reaper interval. The job is then sent back through the
 * normal retry path (or DEAD if attempts are exhausted).
 */
@Component
@Profile("api")
public class Reaper {

    private static final Logger log = LoggerFactory.getLogger(Reaper.class);

    private final JobRepository repo;
    private final JobStateService state;
    private final LeaseStore leases;
    private final Duration grace;
    private final Counter recovered;

    public Reaper(JobRepository repo, JobStateService state, LeaseStore leases, MeterRegistry metrics,
                  @Value("${scheduler.lease-ttl-ms:10000}") long leaseTtlMs) {
        this.repo = repo;
        this.state = state;
        this.leases = leases;
        // A job claimed less than one TTL ago may not have set its lease key yet; skip those.
        this.grace = Duration.ofMillis(leaseTtlMs);
        this.recovered = metrics.counter("scheduler.jobs.recovered");
    }

    @Scheduled(fixedDelayString = "${scheduler.reaper-interval-ms:3000}")
    public void reapExpiredLeases() {
        Instant cutoff = Instant.now().minus(grace);
        for (Job job : repo.findStaleRunning(cutoff, 200)) {
            if (leases.isHeld(job.id())) {
                continue; // worker is alive and heartbeating
            }
            JobStateService.Outcome outcome = state.expire(job);
            if (outcome != JobStateService.Outcome.LEASE_LOST) {
                recovered.increment();
                log.warn("recovered job {} from dead worker {} -> {}", job.id(), job.leaseOwner(), outcome);
            }
        }
    }
}
