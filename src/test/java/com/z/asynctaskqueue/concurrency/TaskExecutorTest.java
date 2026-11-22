package com.z.asynctaskqueue.concurrency;

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
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "async-task.handler.simulate-min-ms=60",
        "async-task.handler.simulate-max-ms=60",
        "async-task.handler.simulate-seed=42",
        "async-task.heartbeat.interval-ms=20"
})
class TaskExecutorTest {

    @Autowired
    private TaskExecutor taskExecutor;

    @Autowired
    private BoundedPriorityBlockingQueue<Task> queue;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskResultRepository taskResultRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private Task newTask(String id, TaskStatus status) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("exec-" + id);
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

    private Task save(Task t) {
        return tx().execute(s -> taskRepository.save(t));
    }

    private Task reload(String id) {
        return tx().execute(s -> taskRepository.findById(id).orElseThrow());
    }

    private Task waitForStatus(String id, TaskStatus expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Task t;
        do {
            t = reload(id);
            if (t.getStatus() == expected) {
                return t;
            }
            Thread.sleep(20);
        } while (System.currentTimeMillis() < deadline);
        return t;
    }

    @Test
    void workerConsumesTask_writesResultAndMarksSuccess() throws Exception {
        Task task = save(newTask("exec-success", TaskStatus.QUEUED));
        LocalDateTime startedBefore = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);

        assertTrue(queue.offer(task), "task must be accepted by the queue");

        Task done = waitForStatus("exec-success", TaskStatus.SUCCESS, 5000);
        assertEquals(TaskStatus.SUCCESS, done.getStatus(), "task must reach SUCCESS");
        assertNotNull(done.getStartedAt());
        assertNotNull(done.getFinishedAt());
        assertTrue(done.getStartedAt().isAfter(startedBefore) || done.getStartedAt().isEqual(startedBefore));
        assertTrue(done.getHeartbeatAt().isAfter(done.getStartedAt()),
                "heartbeat must advance beyond startedAt while processing");

        Optional<TaskResult> result = taskResultRepository.findByTaskId("exec-success");
        assertTrue(result.isPresent(), "a SUCCESS result row must be written in the same transaction");
        assertTrue(result.get().isSuccess());
        assertNotNull(result.get().getResultData());
    }

    @Test
    void duplicateQueueReference_executesExactlyOnce() throws Exception {
        Task task = save(newTask("exec-dup", TaskStatus.QUEUED));

        assertTrue(queue.offer(task));
        assertTrue(queue.offer(task), "offer the same reference twice to simulate a queue duplicate");

        waitForStatus("exec-dup", TaskStatus.SUCCESS, 5000);
        assertEquals(TaskStatus.SUCCESS, reload("exec-dup").getStatus());

        long resultCount = tx().execute(s -> taskResultRepository.findAll().stream()
                .filter(r -> "exec-dup".equals(r.getTaskId())).count());
        assertEquals(1L, resultCount, "duplicate reference must be discarded by the markProcessing guard; only one result row");
    }
}