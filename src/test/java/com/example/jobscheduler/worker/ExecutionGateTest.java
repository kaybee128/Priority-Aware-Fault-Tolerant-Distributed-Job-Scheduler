package com.example.jobscheduler.worker;

import com.example.jobscheduler.common.Priority;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ExecutionGateTest {

    @Test
    void higherPriorityWaiterGetsTheNextFreeSlot() throws Exception {
        ExecutionGate gate = new ExecutionGate(1);
        gate.acquire(Priority.NORMAL); // occupy the only slot

        List<Priority> order = new CopyOnWriteArrayList<>();
        Thread low = new Thread(() -> runOnce(gate, Priority.LOW, order));
        low.start();
        await().untilAsserted(() -> assertThat(gate.waiting(Priority.LOW)).isEqualTo(1));

        Thread high = new Thread(() -> runOnce(gate, Priority.HIGH, order));
        high.start();
        await().untilAsserted(() -> assertThat(gate.waiting(Priority.HIGH)).isEqualTo(1));

        gate.release();
        low.join(5_000);
        high.join(5_000);

        assertThat(order).containsExactly(Priority.HIGH, Priority.LOW);
    }

    @Test
    void slotsAreSharedAcrossPriorities() throws Exception {
        ExecutionGate gate = new ExecutionGate(2);
        gate.acquire(Priority.HIGH);
        gate.acquire(Priority.LOW); // no lane limit: both slots are usable by any priority
        assertThat(gate.waiting(Priority.HIGH)).isZero();
        gate.release();
        gate.release();
    }

    private static void runOnce(ExecutionGate gate, Priority priority, List<Priority> order) {
        try {
            gate.acquire(priority);
            order.add(priority);
            gate.release();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
