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
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class CapacityTuner {

    private static final Logger log = LoggerFactory.getLogger(CapacityTuner.class);
    private static final double HIGH_WATERMARK = 0.8;
    private static final double LOW_WATERMARK = 0.2;
    private static final double TREND_RISING = 1.1;

    private final BoundedPriorityBlockingQueue<Task> queue;
    private final TaskRepository taskRepository;
    private final boolean adaptive;
    private final int minCapacity;
    private final int maxCapacity;
    private final long intervalMs;
    private final long sampleWindowMs;

    private final AtomicBoolean started = new AtomicBoolean();
    private ScheduledExecutorService scheduler;
    private volatile double lastMeanProcessMs = 0d;

    public CapacityTuner(BoundedPriorityBlockingQueue<Task> queue,
                         TaskRepository taskRepository,
                         AsyncTaskProperties properties) {
        this.queue = queue;
        this.taskRepository = taskRepository;
        this.adaptive = properties.getQueue().isAdaptive();
        this.minCapacity = Math.max(1, properties.getQueue().getMinCapacity());
        this.maxCapacity = Math.max(queue.maxCapacity(), properties.getQueue().getMaxCapacity());
        this.intervalMs = Math.max(1000, properties.getQueue().getTunerIntervalMs());
        this.sampleWindowMs = Math.max(intervalMs, properties.getHandler().getSimulateMaxMs());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!adaptive) {
            log.info("CapacityTuner disabled (adaptive=false), queue capacity fixed at {}", queue.currentCapacity());
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "capacity-tuner");
            t.setDaemon(true);
            return t;
        };
        scheduler = Executors.newSingleThreadScheduledExecutor(factory);
        scheduler.scheduleAtFixedRate(this::tune, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("CapacityTuner started, tuning every {}ms (min={}, max={})", intervalMs, minCapacity, maxCapacity);
    }

    private void tune() {
        try {
            int current = queue.currentCapacity();
            int size = queue.size();
            double watermark = current == 0 ? 0d : (double) size / current;
            double meanMs = sampleMeanProcessMs();

            if (watermark > HIGH_WATERMARK && (lastMeanProcessMs == 0d || meanMs > lastMeanProcessMs * TREND_RISING)) {
                int step = Math.max(1, current >> 2);
                int target = Math.min(maxCapacity, current + step);
                if (target > current && queue.setCapacity(target)) {
                    log.info("capacity scaled up {} -> {} (watermark={}, meanMs={})", current, target, watermark, meanMs);
                }
            } else if (watermark < LOW_WATERMARK && meanMs <= lastMeanProcessMs * TREND_RISING && current > minCapacity) {
                int step = Math.max(1, current >> 2);
                int target = Math.max(minCapacity, current - step);
                if (target < current && queue.setCapacity(target)) {
                    log.info("capacity scaled down {} -> {} (watermark={}, meanMs={})", current, target, watermark, meanMs);
                }
            }
            lastMeanProcessMs = meanMs;
        } catch (Exception e) {
            log.warn("capacity tune iteration failed", e);
        }
    }

    private double sampleMeanProcessMs() {
        LocalDateTime after = LocalDateTime.now().minus(Duration.ofMillis(sampleWindowMs));
        List<Task> finished = taskRepository.findByStatusAndFinishedAtAfter(TaskStatus.SUCCESS, after);
        if (finished.isEmpty()) {
            return lastMeanProcessMs;
        }
        long total = 0L;
        int count = 0;
        for (Task t : finished) {
            if (t.getStartedAt() != null && t.getFinishedAt() != null) {
                total += Duration.between(t.getStartedAt(), t.getFinishedAt()).toMillis();
                count++;
            }
        }
        return count == 0 ? lastMeanProcessMs : (double) total / count;
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("CapacityTuner stopped");
        }
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}