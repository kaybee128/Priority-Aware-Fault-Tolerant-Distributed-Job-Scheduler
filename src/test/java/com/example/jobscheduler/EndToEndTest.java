package com.example.jobscheduler;

import com.example.jobscheduler.api.JobView;
import com.example.jobscheduler.common.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the API and worker roles in one JVM against real Postgres, Kafka and Redis.
 * Skipped automatically when Docker is not available.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=api,worker")
@Testcontainers(disabledWithoutDocker = true)
class EndToEndTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void submittedJobRunsToSuccess() {
        ResponseEntity<JobView> response = rest.postForEntity("/api/jobs",
                Map.of("type", "echo", "priority", "HIGH"), JobView.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        UUID id = response.getBody().id();

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            JobView view = rest.getForObject("/api/jobs/" + id, JobView.class);
            assertThat(view.status()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(view.attempts()).isEqualTo(1);
        });
    }

    @Test
    void sameIdempotencyKeyReturnsSameJob() {
        String key = "test-" + UUID.randomUUID();
        Map<String, Object> body = Map.of("type", "echo", "idempotencyKey", key);

        ResponseEntity<JobView> first = rest.postForEntity("/api/jobs", body, JobView.class);
        ResponseEntity<JobView> second = rest.postForEntity("/api/jobs", body, JobView.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().id()).isEqualTo(first.getBody().id());
    }

    @Test
    void jobThatAlwaysFailsEndsDeadAfterMaxAttempts() {
        ResponseEntity<JobView> response = rest.postForEntity("/api/jobs", Map.of(
                "type", "sleep",
                "payload", Map.of("durationMs", 10, "failureRate", 1.0),
                "maxAttempts", 2), JobView.class);
        UUID id = response.getBody().id();

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            JobView view = rest.getForObject("/api/jobs/" + id, JobView.class);
            assertThat(view.status()).isEqualTo(JobStatus.DEAD);
            assertThat(view.attempts()).isEqualTo(2);
        });
    }
}
