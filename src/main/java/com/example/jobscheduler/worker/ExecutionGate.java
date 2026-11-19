package com.example.jobscheduler.worker;

import com.example.jobscheduler.common.Priority;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of execution slots shared by all priority lanes on one worker replica.
 *
 * Any lane can use any free slot, so idle HIGH capacity is not wasted. When slots are
 * scarce, the waiter with the highest priority is granted the next free slot. A running
 * job is never preempted.
 *
 * Listener threads block here before claiming a job, so a job stays QUEUED (not RUNNING)
 * while it waits, and Kafka offsets are committed only after the job finishes.
 */
@Component
@Profile("worker")
public class ExecutionGate {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final int[] waiting = new int[Priority.values().length];
    private int available;

    public ExecutionGate(@Value("${scheduler.worker.slots:8}") int slots) {
        this.available = slots;
    }

    /** Blocks until a slot is free and no higher-priority caller is waiting. */
    public void acquire(Priority priority) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            waiting[priority.ordinal()]++;
            try {
                while (available == 0 || higherPriorityWaiting(priority)) {
                    changed.await();
                }
            } finally {
                waiting[priority.ordinal()]--;
            }
            available--;
        } finally {
            lock.unlock();
        }
    }

    public void release() {
        lock.lock();
        try {
            available++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Number of callers currently waiting for a slot at this priority. */
    int waiting(Priority priority) {
        lock.lock();
        try {
            return waiting[priority.ordinal()];
        } finally {
            lock.unlock();
        }
    }

    // Caller must hold the lock. Lower ordinal means higher priority (HIGH=0).
    private boolean higherPriorityWaiting(Priority priority) {
        for (int i = 0; i < priority.ordinal(); i++) {
            if (waiting[i] > 0) {
                return true;
            }
        }
        return false;
    }
}
