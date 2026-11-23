package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "async-task.heartbeat.timeout-ms=250",
        "async-task.heartbeat.scan-interval-ms=100",
        "async-task.heartbeat.interval-ms=20",
        "async-task.queue.capacity=10",
        "async-task.executor.core-pool-size=2",
        "async-task.handler.simulate-min-ms=30",
        "async-task.handler.simulate-max-ms=60"
})
class HeartbeatRecoveryTest {

    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private RecoveryService recoveryService;
    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private Task save(Task t) {
        return tx().execute(s -> taskRepository.save(t));
    }

    private Task reload(String id) {
        return tx().execute(s -> taskRepository.findById(id).orElseThrow());
    }

    private Task base(String id, TaskStatus status) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("hb-" + id);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(status);
        t.setRetryCount(0);
        t.setMaxRetry(3);
        LocalDateTime now = LocalDateTime.now();
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return t;
    }

    private void waitForStatus(String id, TaskStatus expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (reload(id).getStatus() == expected) {
                return;
            }
            Thread.sleep(50);
        }
    }

    @Test
    void freshHeartbeat_isNotReclaimedButStaleHeartbeat_is() throws Exception {
        Task t = base("hb-fresh", TaskStatus.PROCESSING);
        LocalDateTime now = LocalDateTime.now();
        t.setHeartbeatAt(now);
        t.setStartedAt(now);
        save(t);

        Thread.sleep(150);
        assertEquals(TaskStatus.PROCESSING, reload("hb-fresh").getStatus(),
                "a task with a fresh heartbeat must NOT be reclaimed before timeout (no false kill)");

        waitForStatus("hb-fresh", TaskStatus.RETRY_WAITING, 1000);
        Task result = reload("hb-fresh");
        assertEquals(TaskStatus.RETRY_WAITING, result.getStatus());
        assertEquals(1, result.getRetryCount(), "heartbeat reclaim increments retry count exactly once");
        assertEquals("heartbeat_timeout", result.getFailReason());
    }

    @Test
    void startupRecovery_reEnqueuesPendingQueuedAndDueRetries() throws Exception {
        LocalDateTime now = LocalDateTime.now();

        Task pending = base("hb-rec-pending", TaskStatus.PENDING);
        save(pending);

        Task queued = base("hb-rec-queued", TaskStatus.QUEUED);
        queued.setEnqueuedAt(now);
        save(queued);

        Task retryWaiting = base("hb-rec-retry", TaskStatus.RETRY_WAITING);
        retryWaiting.setRetryCount(1);
        retryWaiting.setNextRetryAt(now.minusSeconds(5));
        save(retryWaiting);

        recoveryService.recover();

        waitForStatus("hb-rec-pending", TaskStatus.SUCCESS, 10000);
        waitForStatus("hb-rec-queued", TaskStatus.SUCCESS, 10000);
        waitForStatus("hb-rec-retry", TaskStatus.SUCCESS, 10000);

        assertEquals(TaskStatus.SUCCESS, reload("hb-rec-pending").getStatus());
        assertEquals(TaskStatus.SUCCESS, reload("hb-rec-queued").getStatus());
        assertEquals(TaskStatus.SUCCESS, reload("hb-rec-retry").getStatus());
        assertNotEquals(0, reload("hb-rec-retry").getRetryCount(),
                "the retried task preserved its pre-recovery retry count");
        assertTrue(reload("hb-rec-retry").getRetryCount() >= 1);
    }
}