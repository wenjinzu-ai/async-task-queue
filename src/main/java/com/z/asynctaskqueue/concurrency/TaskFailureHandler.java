package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.FailureCategory;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;

public class TaskFailureHandler {

    private static final Logger log = LoggerFactory.getLogger(TaskFailureHandler.class);
    private static final int FAIL_REASON_MAX = 1024;

    private final TaskRepository taskRepository;
    private final BackoffCalculator backoffCalculator;
    private final TransactionTemplate tx;

    public TaskFailureHandler(TaskRepository taskRepository,
                              BackoffCalculator backoffCalculator,
                              AsyncTaskProperties properties,
                              PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.backoffCalculator = backoffCalculator;
        this.tx = new TransactionTemplate(transactionManager);
    }

    public void handleFailure(Task task, Throwable error) {
        FailureCategory category = FailureClassifier.classify(error);
        boolean retryable = FailureClassifier.isRetryable(category);
        String reason = truncate(error == null ? "unknown" : error.getMessage());
        tx.executeWithoutResult(s -> transition(task, retryable, reason, category));
    }

    private void transition(Task task, boolean retryable, String reason, FailureCategory category) {
        String taskId = task.getTaskId();
        LocalDateTime now = LocalDateTime.now();

        if (!retryable) {
            int affected = taskRepository.markFailed(taskId, reason, now);
            if (affected == 1) {
                log.info("task {} -> FAILED (category={})", taskId, category);
            }
            return;
        }

        int currentAttempt = task.getRetryCount();
        int maxRetry = task.getMaxRetry();
        if (currentAttempt + 1 >= maxRetry) {
            int affected = taskRepository.markDeadLetter(taskId, reason, now);
            if (affected == 1) {
                log.warn("task {} -> DEAD_LETTER after {} attempts", taskId, currentAttempt + 1);
            }
            return;
        }

        LocalDateTime nextRetryAt = backoffCalculator.nextRetryAt(currentAttempt + 1);
        int affected = taskRepository.markRetryWaiting(taskId, reason, nextRetryAt);
        if (affected == 1) {
            log.info("task {} -> RETRY_WAITING (attempt {} failed, next retry at {})",
                    taskId, currentAttempt + 1, nextRetryAt);
        }
    }

    private String truncate(String message) {
        if (message == null) {
            return "unknown";
        }
        return message.length() > FAIL_REASON_MAX ? message.substring(0, FAIL_REASON_MAX) : message;
    }
}