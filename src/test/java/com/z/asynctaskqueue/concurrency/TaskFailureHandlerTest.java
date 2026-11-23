package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.domain.exception.TaskException;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "async-task.retry.base-backoff-ms=200",
        "async-task.retry.max-backoff-ms=5000",
        "async-task.retry.jitter=false"
})
class TaskFailureHandlerTest {

    @Autowired
    private TaskFailureHandler failureHandler;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private Task processingTask(String id, int retryCount, int maxRetry) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("fh-" + id);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(TaskStatus.PROCESSING);
        t.setRetryCount(retryCount);
        t.setMaxRetry(maxRetry);
        LocalDateTime now = LocalDateTime.now();
        t.setStartedAt(now);
        t.setHeartbeatAt(now);
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return tx().execute(s -> taskRepository.save(t));
    }

    private Task reload(String id) {
        return tx().execute(s -> taskRepository.findById(id).orElseThrow());
    }

    @Test
    void retryableFailure_movesToRetryWaitingAndIncrementsCount() {
        Task task = processingTask("fh-retry", 0, 3);

        failureHandler.handleFailure(task, new TaskException("boom", true));

        Task result = reload("fh-retry");
        assertEquals(TaskStatus.RETRY_WAITING, result.getStatus());
        assertEquals(1, result.getRetryCount());
        assertNotNull(result.getNextRetryAt());
        assertTrue(result.getNextRetryAt().isAfter(LocalDateTime.now()));
        assertEquals("boom", result.getFailReason());
        assertNull(result.getFinishedAt());
    }

    @Test
    void nonRetryableFailure_movesToFailedWithoutCountIncrement() {
        Task task = processingTask("fh-fail", 0, 3);

        failureHandler.handleFailure(task, new TaskException("nope", false));

        Task result = reload("fh-fail");
        assertEquals(TaskStatus.FAILED, result.getStatus());
        assertEquals(0, result.getRetryCount());
        assertNotNull(result.getFinishedAt());
        assertEquals("nope", result.getFailReason());
    }

    @Test
    void unknownRuntimeException_isClassifiedSystemUnrecoverableFailed() {
        Task task = processingTask("fh-sys", 0, 3);

        failureHandler.handleFailure(task, new IllegalStateException("unexpected"));

        assertEquals(TaskStatus.FAILED, reload("fh-sys").getStatus());
    }

    @Test
    void exceedMaxRetry_movesToDeadLetter() {
        Task task = processingTask("fh-dl", 2, 3);

        failureHandler.handleFailure(task, new TaskException("again", true));

        Task result = reload("fh-dl");
        assertEquals(TaskStatus.DEAD_LETTER, result.getStatus());
        assertEquals(3, result.getRetryCount());
        assertNotNull(result.getFinishedAt());
    }

    @Test
    void maxRetryZero_firstRetryableFailureGoesStraightToDeadLetter() {
        Task task = processingTask("fh-noretry", 0, 0);

        failureHandler.handleFailure(task, new TaskException("x", true));

        Task result = reload("fh-noretry");
        assertEquals(TaskStatus.DEAD_LETTER, result.getStatus());
        assertEquals(1, result.getRetryCount());
    }

    @Test
    void transitionDoesNotDoubleCountWhenGuardRejects() {
        Task task = processingTask("fh-idem", 0, 3);
        failureHandler.handleFailure(task, new TaskException("boom", true));

        Task again = reload("fh-idem");
        failureHandler.handleFailure(again, new TaskException("boom", true));

        Task result = reload("fh-idem");
        assertEquals(TaskStatus.RETRY_WAITING, result.getStatus());
        assertEquals(1, result.getRetryCount(), "guard must prevent a second increment when no longer PROCESSING");
    }
}