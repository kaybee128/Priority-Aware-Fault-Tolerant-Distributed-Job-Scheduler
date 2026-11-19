package com.example.jobscheduler.control;

import com.example.jobscheduler.persistence.JobRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Moves SCHEDULED jobs whose run_at has passed onto the queue (via the outbox). */
@Component
@Profile("api")
public class Dispatcher {

    private static final int BATCH_SIZE = 500;

    private final JobRepository repo;

    public Dispatcher(JobRepository repo) {
        this.repo = repo;
    }

    @Scheduled(fixedDelayString = "${scheduler.dispatch-interval-ms:500}")
    @Transactional
    public void dispatchDueJobs() {
        repo.dispatchDue(BATCH_SIZE);
    }
}
