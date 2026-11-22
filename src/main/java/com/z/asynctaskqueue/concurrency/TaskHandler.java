package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskResult;
import com.z.asynctaskqueue.domain.exception.TaskException;
import com.z.asynctaskqueue.repository.TaskRepository;
import com.z.asynctaskqueue.repository.TaskResultRepository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.Random;

public class TaskHandler {

    private final TaskRepository taskRepository;
    private final TaskResultRepository taskResultRepository;
    private final AsyncTaskProperties properties;
    private final TransactionTemplate transactionTemplate;
    private final Random random;

    public TaskHandler(TaskRepository taskRepository,
                       TaskResultRepository taskResultRepository,
                       AsyncTaskProperties properties,
                       PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.taskResultRepository = taskResultRepository;
        this.properties = properties;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        long seed = properties.getHandler().getSimulateSeed();
        this.random = seed == 0 ? new Random() : new Random(seed);
    }

    public void handle(Task task) {
        long simulateMs = computeSimulatedDuration();
        long capMs = properties.getHandler().getCallTimeoutMs();
        boolean timedOut = simulateMs > capMs;
        long workMs = timedOut ? capMs : simulateMs;
        long intervalMs = properties.getHeartbeat().getIntervalMs();

        long elapsed = 0;
        while (elapsed < workMs) {
            long slice = Math.min(intervalMs, workMs - elapsed);
            sleepInterruptibly(slice);
            elapsed += slice;
            if (elapsed < workMs) {
                refreshHeartbeat(task.getTaskId());
            }
        }
        if (timedOut) {
            throw new TaskException("task call timeout after " + capMs + "ms", true);
        }
        commitSuccess(task);
    }

    private long computeSimulatedDuration() {
        AsyncTaskProperties.Handler h = properties.getHandler();
        long min = h.getSimulateMinMs();
        long max = h.getSimulateMaxMs();
        if (max <= min) {
            return min;
        }
        return min + (long) (random.nextDouble() * (max - min));
    }

    private void sleepInterruptibly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TaskException("task interrupted during processing", e, true);
        }
    }

    private void refreshHeartbeat(String taskId) {
        transactionTemplate.executeWithoutResult(s ->
                taskRepository.refreshHeartbeat(taskId, LocalDateTime.now()));
    }

    private void commitSuccess(Task task) {
        transactionTemplate.executeWithoutResult(s -> {
            TaskResult result = new TaskResult();
            result.setTaskId(task.getTaskId());
            result.setResultData(generateResultData(task));
            result.setSuccess(true);
            result.setCreatedAt(LocalDateTime.now());
            taskResultRepository.save(result);

            int affected = taskRepository.markSuccess(task.getTaskId(), LocalDateTime.now());
            if (affected != 1) {
                throw new IllegalStateException(
                        "markSuccess guard returned " + affected + " for task " + task.getTaskId());
            }
        });
    }

    private String generateResultData(Task task) {
        return "{\"taskId\":\"" + task.getTaskId()
                + "\",\"imageUrls\":[\"https://img.example.com/" + task.getTaskId() + ".png\"]}";
    }
}