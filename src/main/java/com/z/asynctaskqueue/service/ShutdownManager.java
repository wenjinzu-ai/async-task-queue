package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.concurrency.BoundedPriorityBlockingQueue;
import com.z.asynctaskqueue.concurrency.CapacityTuner;
import com.z.asynctaskqueue.concurrency.QueueRefiller;
import com.z.asynctaskqueue.concurrency.TaskExecutor;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ShutdownManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ShutdownManager.class);

    private final AcceptanceGate acceptanceGate;
    private final RetryScheduler retryScheduler;
    private final QueueRefiller queueRefiller;
    private final CapacityTuner capacityTuner;
    private final RecoveryService recoveryService;
    private final TaskExecutor taskExecutor;
    private final BoundedPriorityBlockingQueue<Task> queue;
    private final TaskRepository taskRepository;
    private final TransactionTemplate transactionTemplate;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public ShutdownManager(AcceptanceGate acceptanceGate,
                           RetryScheduler retryScheduler,
                           QueueRefiller queueRefiller,
                           CapacityTuner capacityTuner,
                           RecoveryService recoveryService,
                           TaskExecutor taskExecutor,
                           BoundedPriorityBlockingQueue<Task> queue,
                           TaskRepository taskRepository,
                           PlatformTransactionManager transactionManager) {
        this.acceptanceGate = acceptanceGate;
        this.retryScheduler = retryScheduler;
        this.queueRefiller = queueRefiller;
        this.capacityTuner = capacityTuner;
        this.recoveryService = recoveryService;
        this.taskExecutor = taskExecutor;
        this.queue = queue;
        this.taskRepository = taskRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        stop(() -> { });
    }

    @Override
    public void stop(Runnable callback) {
        if (!running.compareAndSet(true, false)) {
            callback.run();
            return;
        }
        log.info("shutdown sequence started");
        try {
            acceptanceGate.shutdown();
            log.info("shutdown: stopped accepting new submissions");

            retryScheduler.stop();
            queueRefiller.stop();
            capacityTuner.stop();
            recoveryService.stop();
            log.info("shutdown: background schedulers stopped");

            taskExecutor.stop();
            log.info("shutdown: worker pool converged");

            drainAndPersist();
        } catch (Exception e) {
            log.error("shutdown sequence encountered error", e);
        } finally {
            log.info("shutdown sequence complete");
            callback.run();
        }
    }

    private void drainAndPersist() {
        List<Task> drained = new ArrayList<>();
        int n = queue.drainTo(drained);
        if (n == 0) {
            return;
        }
        int reverted = transactionTemplate.execute(s -> {
            int count = 0;
            for (Task task : drained) {
                count += taskRepository.revertToPending(task.getTaskId());
            }
            return count;
        });
        log.info("shutdown: drained {} queued tasks from memory and aligned {} to PENDING on disk", n, reverted);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}