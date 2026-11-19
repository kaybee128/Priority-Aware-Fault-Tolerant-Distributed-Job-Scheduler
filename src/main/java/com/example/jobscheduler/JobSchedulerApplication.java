package com.example.jobscheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * One artifact, two roles selected by Spring profile:
 *  - "api":    REST API, outbox publisher, dispatcher (run_at -> queue), reaper (failure recovery)
 *  - "worker": Kafka priority-lane consumers that claim and execute jobs
 */
@SpringBootApplication
@EnableScheduling
public class JobSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(JobSchedulerApplication.class, args);
    }
}
