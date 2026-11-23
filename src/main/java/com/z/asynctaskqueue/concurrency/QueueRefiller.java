package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class QueueRefiller {

    private static final Logger log = LoggerFactory.getLogger(QueueRefiller.class);

    private final TaskRepository taskRepository;
    private final TaskDispatcher dispatcher;
    private final long intervalMs;
    private final int batchSize;

    private final AtomicBoolean started = new AtomicBoolean();
    private ScheduledExecutorService scheduler;

    public QueueRefiller(TaskRepository taskRepository,
                         TaskDispatcher dispatcher,
                         AsyncTaskProperties properties) {
        this.taskRepository = taskRepository;
        this.dispatcher = dispatcher;
        this.intervalMs = Math.max(50, properties.getQueue().getRefillerIntervalMs());
        this.batchSize = Math.max(1, properties.getQueue().getRefillerBatchSize());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "queue-refiller");
            t.setDaemon(true);
            return t;
        };
        scheduler = Executors.newSingleThreadScheduledExecutor(factory);
        scheduler.scheduleAtFixedRate(this::refill, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("QueueRefiller started, scanning PENDING every {}ms (batch {})", intervalMs, batchSize);
    }

    private void refill() {
        try {
            if (dispatcher.remainingCapacity() <= 0) {
                return;
            }
            Pageable page = PageRequest.of(0, batchSize,
                    Sort.by(Sort.Direction.ASC, "createdAt"));
            List<Task> pending = taskRepository.findByStatus(TaskStatus.PENDING, page).getContent();
            for (Task task : pending) {
                if (dispatcher.remainingCapacity() <= 0) {
                    break;
                }
                EnqueueOutcome outcome = dispatcher.enqueueFromPending(task);
                if (outcome == EnqueueOutcome.ENQUEUED) {
                    log.debug("refilled task {} into queue", task.getTaskId());
                }
            }
        } catch (Exception e) {
            log.warn("refill iteration failed", e);
        }
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("QueueRefiller stopped");
        }
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}