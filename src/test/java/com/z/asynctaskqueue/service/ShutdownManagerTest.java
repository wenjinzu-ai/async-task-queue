package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.api.dto.SubmitRequest;
import com.z.asynctaskqueue.concurrency.BoundedPriorityBlockingQueue;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.domain.exception.BusinessException;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "async-task.queue.capacity=4",
        "async-task.queue.offer-timeout-ms=30",
        "async-task.queue.backpressure=REJECT",
        "async-task.executor.core-pool-size=1",
        "async-task.executor.max-pool-size=1",
        "async-task.handler.simulate-min-ms=400",
        "async-task.handler.simulate-max-ms=500",
        "async-task.shutdown.await-termination-ms=5000"
})
class ShutdownManagerTest {

    @Autowired
    private TaskService taskService;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private ShutdownManager shutdownManager;
    @Autowired
    private AcceptanceGate acceptanceGate;
    @Autowired
    private BoundedPriorityBlockingQueue<Task> queue;
    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private SubmitRequest req(String bizKey) {
        SubmitRequest r = new SubmitRequest();
        r.setBizKey(bizKey);
        r.setTaskType("IMAGE");
        r.setPriority(5);
        r.setPayload("{}");
        return r;
    }

    private int countByStatus(TaskStatus status) {
        return (int) tx().execute(s -> taskRepository.findByStatus(status)).stream().count();
    }

    private void waitForQueuedAtLeast(int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (countByStatus(TaskStatus.QUEUED) >= expected) {
                return;
            }
            Thread.sleep(50);
        }
    }

    @Test
    void stop_drainsMemoryQueueAndAlignsToDisk() throws Exception {
        int submitted = 0;
        for (int i = 0; i < 6; i++) {
            try {
                taskService.submit(req("sd-" + i));
                submitted++;
            } catch (BusinessException e) {
                // REJECT backpressure: task persisted as PENDING, expected for the overflow task
            }
        }
        assertTrue(submitted >= 5, "at least 5 submissions should succeed before backpressure kicks in");

        waitForQueuedAtLeast(3, 2000);
        int queuedBefore = countByStatus(TaskStatus.QUEUED);
        assertTrue(queuedBefore >= 1, "there must be tasks buffered in the memory queue before shutdown");

        shutdownManager.stop();

        assertEquals(0, queue.size(), "memory queue must be drained on shutdown");
        assertFalse(acceptanceGate.isAccepting(), "gate must reject new submissions after shutdown");
        assertEquals(0, countByStatus(TaskStatus.QUEUED),
                "no task may be left in QUEUED with no in-memory reference after drain-to-persist");

        List<Task> pending = tx().execute(s -> taskRepository.findByStatus(TaskStatus.PENDING));
        for (Task t : pending) {
            assertEquals(TaskStatus.PENDING, t.getStatus(),
                    "drained task " + t.getTaskId() + " must be aligned back to PENDING on disk");
        }
        assertTrue(pending.size() >= 1, "the drained queued tasks must reappear as PENDING for restart recovery");
    }
}