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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class ResultCleanupJobTest {

    @Autowired
    private ResultCleanupJob cleanupJob;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskResultRepository resultRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private Task successTask(String id, LocalDateTime finishedAt) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("clean-" + id);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(TaskStatus.SUCCESS);
        t.setRetryCount(0);
        t.setMaxRetry(3);
        t.setFinishedAt(finishedAt);
        LocalDateTime now = LocalDateTime.now();
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return tx().execute(s -> taskRepository.save(t));
    }

    private void saveResult(String taskId, String data) {
        TaskResult r = new TaskResult();
        r.setTaskId(taskId);
        r.setSuccess(true);
        r.setResultData(data);
        r.setCreatedAt(LocalDateTime.now());
        tx().executeWithoutResult(s -> resultRepository.save(r));
    }

    @Test
    void deletesResultsForSuccessTasksOlderThanRetention() {
        successTask("clean-old", LocalDateTime.now().minusDays(31));
        saveResult("clean-old", "old-data");
        successTask("clean-recent", LocalDateTime.now());
        saveResult("clean-recent", "recent-data");

        cleanupJob.cleanupExpiredResults();

        assertFalse(resultRepository.findByTaskId("clean-old").isPresent(),
                "result older than retention must be deleted");
        assertTrue(resultRepository.findByTaskId("clean-recent").isPresent(),
                "recent result must be retained");
        assertTrue(taskRepository.findById("clean-old").isPresent(),
                "the task row itself must remain for historical stats");
    }
}