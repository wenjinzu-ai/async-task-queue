package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskResult;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import com.z.asynctaskqueue.repository.TaskResultRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "async-task.queue.capacity=3",
        "async-task.queue.refiller-interval-ms=150",
        "async-task.queue.refiller-batch-size=10",
        "async-task.executor.core-pool-size=2",
        "async-task.handler.simulate-min-ms=40",
        "async-task.handler.simulate-max-ms=80"
})
class QueueRefillerIntegrationTest {

    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private TaskResultRepository resultRepository;
    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private Task pendingTask(String id) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("rf-" + id);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(TaskStatus.PENDING);
        t.setRetryCount(0);
        t.setMaxRetry(3);
        LocalDateTime now = LocalDateTime.now();
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return tx().execute(s -> taskRepository.save(t));
    }

    private Task reload(String id) {
        return tx().execute(s -> taskRepository.findById(id).orElseThrow());
    }

    private void waitForAllSuccess(String prefix, int count, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allDone = true;
            for (int i = 0; i < count; i++) {
                if (reload(prefix + i).getStatus() != TaskStatus.SUCCESS) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                return;
            }
            Thread.sleep(100);
        }
    }

    @Test
    void pendingTasks_areRefilledAndEventuallySucceed() throws Exception {
        int total = 10;
        for (int i = 0; i < total; i++) {
            pendingTask("rf-task-" + i);
        }

        waitForAllSuccess("rf-task-", total, 10000);

        for (int i = 0; i < total; i++) {
            assertEquals(TaskStatus.SUCCESS, reload("rf-task-" + i).getStatus(),
                    "refiller must catch up every backpressured PENDING task: " + i);
            assertTrue(resultRepository.findByTaskId("rf-task-" + i).isPresent(),
                    "result row must exist for " + i);
        }
        assertEquals(total, resultRepository.findAll().size(), "exactly one result per task, no duplicates");
    }
}