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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class RetrySchedulerIntegrationTest {

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskResultRepository taskResultRepository;

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

    private Task retryWaitingTask(String id, LocalDateTime nextRetryAt, int retryCount) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("rs-" + id);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(TaskStatus.RETRY_WAITING);
        t.setRetryCount(retryCount);
        t.setMaxRetry(3);
        t.setNextRetryAt(nextRetryAt);
        LocalDateTime now = LocalDateTime.now();
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return t;
    }

    private Task waitForStatus(String id, TaskStatus expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Task t;
        do {
            t = reload(id);
            if (t.getStatus() == expected) {
                return t;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);
        return t;
    }

    @Test
    void dueRetryWaitingTask_isReleasedAndCompletesSuccessfully() throws Exception {
        save(retryWaitingTask("rs-due", LocalDateTime.now().minusSeconds(5), 1));

        Task done = waitForStatus("rs-due", TaskStatus.SUCCESS, 6000);
        assertEquals(TaskStatus.SUCCESS, done.getStatus());

        Optional<TaskResult> result = taskResultRepository.findByTaskId("rs-due");
        assertTrue(result.isPresent(), "the retried task must be processed and write a result row");
        assertTrue(result.get().isSuccess());
    }

    @Test
    void futureRetryWaitingTask_isNotReleasedBeforeNextRetryAt() throws Exception {
        save(retryWaitingTask("rs-future", LocalDateTime.now().plusSeconds(30), 1));

        Thread.sleep(2500);

        assertEquals(TaskStatus.RETRY_WAITING, reload("rs-future").getStatus(),
                "a task whose nextRetryAt is in the future must not be released early");
    }
}