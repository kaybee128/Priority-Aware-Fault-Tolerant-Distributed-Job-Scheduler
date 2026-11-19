package com.example.jobscheduler.common;

/**
 * SCHEDULED -> (run_at reached) -> QUEUED -> (worker claims) -> RUNNING
 *   RUNNING -> SUCCEEDED
 *   RUNNING -> SCHEDULED   (failed with attempts left, or lease expired; retry later)
 *   RUNNING -> DEAD        (failed with no attempts left; copied to jobs.dlq)
 */
public enum JobStatus {
    SCHEDULED, QUEUED, RUNNING, SUCCEEDED, DEAD;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD;
    }
}
