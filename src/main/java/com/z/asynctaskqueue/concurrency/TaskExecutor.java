package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);
    private static final long WORKER_POLL_TIMEOUT_MS = 1000L;

    private final BoundedPriorityBlockingQueue<Task> queue;
    private final TaskHandler taskHandler;
    private final TaskRepository taskRepository;
    private final TaskFailureHandler failureHandler;
    private final TransactionTemplate transactionTemplate;
    private final int corePoolSize;
    private final String threadNamePrefix;
    private final long awaitTerminationMs;

    private final AtomicInteger workerSeq = new AtomicInteger();
    private final AtomicInteger activeWorkers = new AtomicInteger();
    private final Object lifecycleLock = new Object();
    private volatile boolean running = false;
    private volatile boolean started = false;
    private ThreadPoolExecutor executor;

    public TaskExecutor(BoundedPriorityBlockingQueue<Task> queue,
                        TaskHandler taskHandler,
                        TaskRepository taskRepository,
                        TaskFailureHandler failureHandler,
                        AsyncTaskProperties properties,
                        PlatformTransactionManager transactionManager) {
        this.queue = queue;
        this.taskHandler = taskHandler;
        this.taskRepository = taskRepository;
        this.failureHandler = failureHandler;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.corePoolSize = Math.max(1, properties.getExecutor().getCorePoolSize());
        this.threadNamePrefix = properties.getExecutor().getThreadNamePrefix();
        this.awaitTerminationMs = properties.getShutdown().getAwaitTerminationMs();
    }

    public void start() {
        synchronized (lifecycleLock) {
            if (started) {
                return;
            }
            running = true;
            started = true;
            ThreadFactory factory = r -> {
                Thread t = new Thread(r, threadNamePrefix + "-" + workerSeq.incrementAndGet());
                t.setUncaughtExceptionHandler((th, ex) ->
                        log.error("worker {} uncaught exception", th.getName(), ex));
                return t;
            };
            executor = new ThreadPoolExecutor(corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), factory);
            for (int i = 0; i < corePoolSize; i++) {
                spawnWorker();
            }
            log.info("TaskExecutor started with {} workers", corePoolSize);
        }
    }

    private void spawnWorker() {
        activeWorkers.incrementAndGet();
        try {
            executor.execute(() -> {
                try {
                    workerLoop();
                } finally {
                    activeWorkers.decrementAndGet();
                    if (running) {
                        log.warn("worker exited unexpectedly while running; restarting a replacement");
                        spawnWorker();
                    }
                }
            });
        } catch (RuntimeException e) {
            activeWorkers.decrementAndGet();
            log.error("failed to spawn worker", e);
        }
    }

    private void workerLoop() {
        while (running) {
            Task task;
            try {
                task = queue.poll(WORKER_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (task == null) {
                continue;
            }
            processTask(task);
        }
    }

    private void processTask(Task task) {
        try {
            int claimed = transactionTemplate.execute(s ->
                    taskRepository.markProcessing(task.getTaskId(), LocalDateTime.now()));
            if (claimed != 1) {
                // Duplicate reference in the queue or already claimed by another worker: discard without executing.
                log.debug("task {} not claimed (guard={}), discarding duplicate reference", task.getTaskId(), claimed);
                return;
            }
            taskHandler.handle(task);
        } catch (Exception e) {
            log.error("task {} failed during handling", task.getTaskId(), e);
            try {
                failureHandler.handleFailure(task, e);
            } catch (RuntimeException fe) {
                // A failure in the transition itself must not kill the worker; the task stays
                // PROCESSING and will be recovered by heartbeat timeout (ATQ-019).
                log.error("task {} failure transition itself failed; leaving for heartbeat recovery",
                        task.getTaskId(), fe);
            }
        }
    }

    public void stop() {
        synchronized (lifecycleLock) {
            if (!started) {
                return;
            }
            running = false;
            started = false;
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(awaitTerminationMs, TimeUnit.MILLISECONDS)) {
                    log.warn("workers did not terminate within {}ms, forcing shutdown", awaitTerminationMs);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("TaskExecutor stopped");
    }

    public int getActiveWorkers() {
        return activeWorkers.get();
    }
}