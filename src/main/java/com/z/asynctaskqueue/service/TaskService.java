package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.api.dto.SubmitRequest;
import com.z.asynctaskqueue.api.dto.SubmitResponse;
import com.z.asynctaskqueue.concurrency.BackpressureStrategy;
import com.z.asynctaskqueue.concurrency.EnqueueOutcome;
import com.z.asynctaskqueue.concurrency.TaskDispatcher;
import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.domain.exception.BusinessException;
import com.z.asynctaskqueue.domain.exception.ErrorCode;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
public class TaskService {

    private final TaskRepository taskRepository;
    private final TaskDispatcher dispatcher;
    private final AcceptanceGate acceptanceGate;
    private final AsyncTaskProperties properties;
    private final TransactionTemplate transactionTemplate;

    public TaskService(TaskRepository taskRepository,
                       TaskDispatcher dispatcher,
                       AcceptanceGate acceptanceGate,
                       AsyncTaskProperties properties,
                       PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.dispatcher = dispatcher;
        this.acceptanceGate = acceptanceGate;
        this.properties = properties;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public SubmitResponse submit(SubmitRequest request) {
        acceptanceGate.checkAccepting();
        String bizKey = request.getBizKey();

        Optional<Task> existing = readByBizKey(bizKey);
        if (existing.isPresent()) {
            return onExistingTask(existing.get(), request);
        }

        Task task = buildTask(request, bizKey);
        try {
            Task persisted = tx(s -> taskRepository.save(task));
            return afterPersist(persisted);
        } catch (DataIntegrityViolationException e) {
            // TOCTOU: a concurrent insert won the unique index. Fall back to the existing row and apply
            // the same idempotency rules instead of surfacing a 500 (SDD 6.1 / 9.3).
            Task winner = readByBizKey(bizKey).orElseThrow(() -> e);
            return onExistingTask(winner, request);
        }
    }

    private SubmitResponse onExistingTask(Task task, SubmitRequest request) {
        if (isFinal(task.getStatus())) {
            if (properties.getSubmit().isAllowResubmitWhenFinal()) {
                Task resubmitted = buildTask(request, versionedBizKey(task.getBizKey()));
                Task persisted = tx(s -> taskRepository.save(resubmitted));
                return afterPersist(persisted);
            }
            throw new BusinessException(ErrorCode.TASK_ALREADY_FINALIZED,
                    "task already finalized: " + task.getTaskId(), task.getTaskId());
        }
        return SubmitResponse.of(task.getTaskId(), task.getStatus());
    }

    private SubmitResponse afterPersist(Task task) {
        EnqueueOutcome outcome = dispatcher.enqueueFromPending(task);
        switch (outcome) {
            case ENQUEUED:
            case ALREADY_QUEUED:
                return SubmitResponse.of(task.getTaskId(), TaskStatus.QUEUED);
            case REJECTED:
                return onBackpressure(task);
            default:
                throw new IllegalStateException("unexpected enqueue outcome: " + outcome);
        }
    }

    private SubmitResponse onBackpressure(Task task) {
        BackpressureStrategy strategy = BackpressureStrategy.from(properties.getQueue().getBackpressure());
        if (strategy == BackpressureStrategy.REJECT) {
            throw new BusinessException(ErrorCode.QUEUE_FULL,
                    "queue full; task persisted as PENDING and will be refilled automatically", task.getTaskId());
        }
        // PERSIST_ONLY: silently keep PENDING, report 200. BLOCK is internal-only and not used on the submit path.
        return SubmitResponse.of(task.getTaskId(), TaskStatus.PENDING);
    }

    public TaskStatus replayDeadLetter(String taskId) {
        Task task = tx(s -> taskRepository.findById(taskId))
                .orElseThrow(() -> new BusinessException(ErrorCode.TASK_NOT_FOUND,
                        "task not found: " + taskId, taskId));
        if (task.getStatus() != TaskStatus.DEAD_LETTER) {
            throw new BusinessException(ErrorCode.TASK_STATE_CONFLICT,
                    "task is not in DEAD_LETTER state: " + task.getStatus(), taskId);
        }
        int reset = tx(s -> taskRepository.replayDeadLetter(taskId));
        if (reset != 1) {
            throw new BusinessException(ErrorCode.TASK_STATE_CONFLICT,
                    "task state changed concurrently, cannot replay", taskId);
        }
        Task reloaded = tx(s -> taskRepository.findById(taskId)).orElseThrow();
        EnqueueOutcome outcome = dispatcher.enqueueFromPending(reloaded);
        return outcome == EnqueueOutcome.REJECTED ? TaskStatus.PENDING : TaskStatus.QUEUED;
    }

    private Task buildTask(SubmitRequest request, String bizKey) {
        LocalDateTime now = LocalDateTime.now();
        Task task = new Task();
        task.setTaskId(UUID.randomUUID().toString());
        task.setBizKey(bizKey);
        task.setTaskType(request.getTaskType());
        task.setPriority(request.getPriority());
        task.setPayload(request.getPayload());
        task.setStatus(TaskStatus.PENDING);
        task.setRetryCount(0);
        task.setMaxRetry(properties.getRetry().getMaxRetry());
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        return task;
    }

    private String versionedBizKey(String original) {
        return original + "#" + UUID.randomUUID();
    }

    private boolean isFinal(TaskStatus status) {
        return status == TaskStatus.SUCCESS
                || status == TaskStatus.FAILED
                || status == TaskStatus.DEAD_LETTER;
    }

    private Optional<Task> readByBizKey(String bizKey) {
        return tx(s -> taskRepository.findByBizKey(bizKey));
    }

    private <R> R tx(org.springframework.transaction.support.TransactionCallback<R> action) {
        return transactionTemplate.execute(action);
    }
}