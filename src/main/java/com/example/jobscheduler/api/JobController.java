package com.example.jobscheduler.api;

import com.example.jobscheduler.persistence.JobRepository;
import com.example.jobscheduler.service.JobSubmissionService;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
@Profile("api")
public class JobController {

    private final JobSubmissionService submissions;
    private final JobRepository repo;

    public JobController(JobSubmissionService submissions, JobRepository repo) {
        this.submissions = submissions;
        this.repo = repo;
    }

    /** 202 when a new job was accepted, 200 when the idempotency key matched an existing job. */
    @PostMapping
    public ResponseEntity<JobView> submit(@Valid @RequestBody JobRequest request) {
        JobSubmissionService.Result result = submissions.submit(request);
        HttpStatus status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(JobView.from(result.job()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobView> get(@PathVariable UUID id) {
        return repo.findById(id)
                .map(job -> ResponseEntity.ok(JobView.from(job)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return repo.countByStatus();
    }
}
