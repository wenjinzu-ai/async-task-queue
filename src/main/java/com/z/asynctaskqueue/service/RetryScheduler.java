package com.z.asynctaskqueue.service;

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

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);
    private static final long SCAN_INTERVAL_MS = 1000L;

    private final TaskRepository taskRepository;
    private final TaskDispatcher dispatcher;
    private final int poolSize;

    private final AtomicBoolean started = new AtomicBoolean();
    private ScheduledExecutorService scheduler;

    public RetryScheduler(TaskRepository taskRepository,
                          TaskDispatcher dispatcher,
                          AsyncTaskProperties properties) {
        this.taskRepository = taskRepository;
        this.dispatcher = dispatcher;
        this.poolSize = Math.max(1, properties.getRetry().getSchedulerPoolSize());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "retry-scheduler");
            t.setDaemon(true);
            return t;
        };
        scheduler = Executors.newScheduledThreadPool(poolSize, factory);
        scheduler.scheduleAtFixedRate(this::scanDueRetries,
                SCAN_INTERVAL_MS, SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("RetryScheduler started, scanning due retries every {}ms", SCAN_INTERVAL_MS);
    }

    private void scanDueRetries() {
        try {
            if (dispatcher.remainingCapacity() <= 0) {
                return;
            }
            LocalDateTime now = LocalDateTime.now();
            List<Task> due = taskRepository.findByStatusAndNextRetryAtBefore(TaskStatus.RETRY_WAITING, now);
            for (Task task : due) {
                if (dispatcher.remainingCapacity() <= 0) {
                    break;
                }
                dispatcher.enqueueFromRetryWaiting(task);
            }
        } catch (Exception e) {
            log.warn("retry scan iteration failed", e);
        }
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("RetryScheduler stopped");
        }
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}