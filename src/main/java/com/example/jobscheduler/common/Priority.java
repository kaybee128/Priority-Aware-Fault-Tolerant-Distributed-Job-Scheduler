package com.example.jobscheduler.common;

import java.util.Locale;

public enum Priority {
    HIGH, NORMAL, LOW;

    /** Kafka topic for this priority lane, e.g. {@code jobs.high}. */
    public String topic() {
        return "jobs." + name().toLowerCase(Locale.ROOT);
    }
}
