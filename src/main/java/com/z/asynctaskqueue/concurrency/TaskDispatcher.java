package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final BoundedPriorityBlockingQueue<Task> queue;
    private final TaskRepository taskRepository;
    private final TransactionTemplate transactionTemplate;
    private final long offerTimeoutMs;

    public TaskDispatcher(BoundedPriorityBlockingQueue<Task> queue,
                          TaskRepository taskRepository,
                          AsyncTaskProperties properties,
                          PlatformTransactionManager transactionManager) {
        this.queue = queue;
        this.taskRepository = taskRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.offerTimeoutMs = properties.getQueue().getOfferTimeoutMs();
    }

    public EnqueueOutcome enqueueFromPending(Task task) {
        LocalDateTime now = LocalDateTime.now();
        int updated = transactionTemplate.execute(s -> taskRepository.markQueuedFromPending(task.getTaskId(), now));
        if (updated != 1) {
            return EnqueueOutcome.ALREADY_QUEUED;
        }
        if (doOffer(task)) {
            return EnqueueOutcome.ENQUEUED;
        }
        // Backpressure: the DB was flipped to QUEUED but the memory queue is full. Revert to PENDING
        // so QueueRefiller can re-enqueue it later -- otherwise the task would be stuck in QUEUED
        // with no in-memory reference and never consumed (non-loss invariant, SDD 6.1/6.9).
        transactionTemplate.executeWithoutResult(s -> taskRepository.revertToPending(task.getTaskId()));
        log.debug("task {} backpressured, reverted to PENDING for refiller", task.getTaskId());
        return EnqueueOutcome.REJECTED;
    }

    private boolean doOffer(Task task) {
        try {
            return queue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public int queueSize() {
        return queue.size();
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }
}