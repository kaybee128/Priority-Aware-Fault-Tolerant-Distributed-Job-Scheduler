package com.example.jobscheduler.control;

import com.example.jobscheduler.persistence.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

/**
 * Transactional-outbox relay: publishes pending outbox rows to Kafka.
 * A crash between send and commit causes a duplicate publish (at-least-once);
 * workers tolerate that because the claim is conditional on QUEUED.
 */
@Component
@Profile("api")
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JobRepository repo;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPublisher(JobRepository repo, KafkaTemplate<String, String> kafka) {
        this.repo = repo;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelayString = "${scheduler.outbox-interval-ms:200}")
    @Transactional
    public void publishPending() {
        for (JobRepository.OutboxRow row : repo.lockUnpublished(200)) {
            try {
                kafka.send(row.topic(), row.key(), row.value()).get(10, TimeUnit.SECONDS);
                repo.markPublished(row.id());
            } catch (Exception e) {
                // Stop this round; rows already marked stay published, the rest retry next tick.
                log.warn("outbox publish failed for row {} (will retry): {}", row.id(), e.toString());
                return;
            }
        }
    }
}
