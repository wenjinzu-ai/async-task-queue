package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.concurrency.EnqueueOutcome;
import com.z.asynctaskqueue.concurrency.TaskDispatcher;
import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final TaskRepository taskRepository;
    private final TaskDispatcher dispatcher;
    private final long heartbeatTimeoutMs;
    private final long scanIntervalMs;
    private final TransactionTemplate transactionTemplate;

    private final AtomicBoolean started = new AtomicBoolean();
    private ScheduledExecutorService scheduler;

    public RecoveryService(TaskRepository taskRepository,
                           TaskDispatcher dispatcher,
                           AsyncTaskProperties properties,
                           PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.dispatcher = dispatcher;
        this.heartbeatTimeoutMs = properties.getHeartbeat().getTimeoutMs();
        this.scanIntervalMs = Math.max(1000, properties.getHeartbeat().getScanIntervalMs());
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        recover();
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "recovery-scan");
            t.setDaemon(true);
            return t;
        };
        scheduler = Executors.newSingleThreadScheduledExecutor(factory);
        scheduler.scheduleAtFixedRate(this::scanHeartbeatTimeouts,
                scanIntervalMs, scanIntervalMs, TimeUnit.MILLISECONDS);
        log.info("RecoveryService started, heartbeat scan every {}ms (timeout {}ms)", scanIntervalMs, heartbeatTimeoutMs);
    }

    public void recover() {
        try {
            int recovered = 0;
            LocalDateTime now = LocalDateTime.now();
            for (Task task : taskRepository.findByStatus(TaskStatus.PENDING)) {
                EnqueueOutcome o = dispatcher.enqueueFromPending(task);
                if (o == EnqueueOutcome.ENQUEUED) recovered++;
            }
            for (Task task : taskRepository.findByStatus(TaskStatus.QUEUED)) {
                EnqueueOutcome o = dispatcher.requeueQueued(task);
                if (o == EnqueueOutcome.ENQUEUED) recovered++;
            }
            for (Task task : taskRepository.findByStatusAndNextRetryAtBefore(TaskStatus.RETRY_WAITING, now)) {
                EnqueueOutcome o = dispatcher.enqueueFromRetryWaiting(task);
                if (o == EnqueueOutcome.ENQUEUED) recovered++;
            }
            if (recovered > 0) {
                log.info("startup recovery re-enqueued {} tasks", recovered);
            }
        } catch (Exception e) {
            log.warn("startup recovery failed", e);
        }
    }

    private void scanHeartbeatTimeouts() {
        try {
            LocalDateTime threshold = LocalDateTime.now().minusNanos(heartbeatTimeoutMs * 1_000_000L);
            List<Task> stale = taskRepository.findByStatusAndHeartbeatAtBefore(TaskStatus.PROCESSING, threshold);
            if (stale.isEmpty()) {
                return;
            }
            LocalDateTime now = LocalDateTime.now();
            int reclaimed = 0;
            for (Task task : stale) {
                int updated = transactionTemplate.execute(s ->
                        taskRepository.recoverHeartbeatTimeout(task.getTaskId(), now, threshold));
                if (updated == 1) {
                    reclaimed++;
                    log.warn("task {} reclaimed: heartbeat stale (< {}) -> RETRY_WAITING",
                            task.getTaskId(), threshold);
                }
            }
            if (reclaimed > 0) {
                log.info("heartbeat scan reclaimed {} stale tasks", reclaimed);
            }
        } catch (Exception e) {
            log.warn("heartbeat scan iteration failed", e);
        }
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("RecoveryService stopped");
        }
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}