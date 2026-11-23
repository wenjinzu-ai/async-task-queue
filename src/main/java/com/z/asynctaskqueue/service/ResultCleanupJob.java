package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import com.z.asynctaskqueue.repository.TaskResultRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class ResultCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(ResultCleanupJob.class);
    private static final String CLEANUP_CRON = "0 0 3 * * *";

    private final TaskRepository taskRepository;
    private final TaskResultRepository resultRepository;
    private final AsyncTaskProperties properties;
    private final TransactionTemplate tx;

    public ResultCleanupJob(TaskRepository taskRepository,
                            TaskResultRepository resultRepository,
                            AsyncTaskProperties properties,
                            PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.resultRepository = resultRepository;
        this.properties = properties;
        this.tx = new TransactionTemplate(transactionManager);
    }

    @Scheduled(cron = CLEANUP_CRON)
    public void cleanupExpiredResults() {
        int retentionDays = properties.getCleanup().getResultRetentionDays();
        if (retentionDays <= 0) {
            return;
        }
        LocalDateTime threshold = LocalDateTime.now().minusDays(retentionDays);
        List<Task> expired = tx.execute(s ->
                taskRepository.findByStatusAndFinishedAtBefore(TaskStatus.SUCCESS, threshold));
        if (expired == null || expired.isEmpty()) {
            return;
        }
        List<String> taskIds = expired.stream().map(Task::getTaskId).toList();
        tx.executeWithoutResult(s -> resultRepository.deleteByTaskIdIn(taskIds));
        log.info("cleaned up {} expired task_result rows (retention {} days)", taskIds.size(), retentionDays);
    }
}