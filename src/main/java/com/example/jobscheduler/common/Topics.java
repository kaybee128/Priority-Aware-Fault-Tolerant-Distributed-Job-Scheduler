package com.example.jobscheduler.common;

public final class Topics {

    public static final String HIGH = "jobs.high";
    public static final String NORMAL = "jobs.normal";
    public static final String LOW = "jobs.low";
    public static final String DLQ = "jobs.dlq";

    public static final String CONSUMER_GROUP = "job-workers";

    private Topics() {
    }
}
