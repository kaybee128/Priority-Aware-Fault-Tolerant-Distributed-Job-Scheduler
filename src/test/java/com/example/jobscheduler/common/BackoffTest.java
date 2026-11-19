package com.example.jobscheduler.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffTest {

    @Test
    void doublesEachAttemptAndCapsAtMax() {
        Backoff backoff = new Backoff(2000, 60000);

        assertThat(backoff.delayAfter(1)).isEqualTo(Duration.ofMillis(2000));
        assertThat(backoff.delayAfter(2)).isEqualTo(Duration.ofMillis(4000));
        assertThat(backoff.delayAfter(3)).isEqualTo(Duration.ofMillis(8000));
        assertThat(backoff.delayAfter(50)).isEqualTo(Duration.ofMillis(60000));
    }
}
