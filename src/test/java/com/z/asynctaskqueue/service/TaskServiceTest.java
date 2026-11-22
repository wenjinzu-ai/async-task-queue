package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.api.dto.SubmitRequest;
import com.z.asynctaskqueue.api.dto.SubmitResponse;
import com.z.asynctaskqueue.concurrency.EnqueueOutcome;
import com.z.asynctaskqueue.concurrency.TaskDispatcher;
import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.domain.exception.BusinessException;
import com.z.asynctaskqueue.domain.exception.ErrorCode;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskServiceTest {

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private TaskDispatcher dispatcher;

    @Mock
    private PlatformTransactionManager transactionManager;

    private AcceptanceGate acceptanceGate;
    private AsyncTaskProperties properties;
    private TaskService service;

    @BeforeEach
    void setUp() {
        acceptanceGate = new AcceptanceGate();
        properties = new AsyncTaskProperties();
        properties.getRetry().setMaxRetry(3);
        lenient().when(transactionManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
        service = new TaskService(taskRepository, dispatcher, acceptanceGate, properties, transactionManager);
    }

    private SubmitRequest req(String bizKey) {
        SubmitRequest r = new SubmitRequest();
        r.setBizKey(bizKey);
        r.setTaskType("IMAGE");
        r.setPriority(5);
        r.setPayload("{}");
        return r;
    }

    private Task persisted(String id, String bizKey, TaskStatus status) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey(bizKey);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(status);
        t.setRetryCount(0);
        t.setMaxRetry(3);
        return t;
    }

    @Test
    void newTask_persistsAndEnqueues_returnsQueued() {
        when(taskRepository.findByBizKey("k1")).thenReturn(Optional.empty());
        when(taskRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(dispatcher.enqueueFromPending(any())).thenReturn(EnqueueOutcome.ENQUEUED);

        SubmitResponse resp = service.submit(req("k1"));

        assertNotNull(resp.getTaskId());
        assertEquals(TaskStatus.QUEUED, resp.getStatus());
        verify(dispatcher).enqueueFromPending(any());
    }

    @Test
    void sameBizKeyInFlight_returnsSameIdWithoutReEnqueue() {
        when(taskRepository.findByBizKey("k2"))
                .thenReturn(Optional.of(persisted("id-2", "k2", TaskStatus.QUEUED)));

        SubmitResponse resp = service.submit(req("k2"));

        assertEquals("id-2", resp.getTaskId());
        assertEquals(TaskStatus.QUEUED, resp.getStatus());
        verify(taskRepository, never()).save(any());
        verify(dispatcher, never()).enqueueFromPending(any());
    }

    @Test
    void finalBizKey_resubmitDisabled_throwsConflict() {
        when(taskRepository.findByBizKey("k3"))
                .thenReturn(Optional.of(persisted("id-3", "k3", TaskStatus.SUCCESS)));

        BusinessException ex = assertThrows(BusinessException.class, () -> service.submit(req("k3")));

        assertEquals(ErrorCode.TASK_ALREADY_FINALIZED, ex.getCode());
        assertEquals("id-3", ex.getTaskId());
    }

    @Test
    void finalBizKey_resubmitEnabled_createsNewTaskAndEnqueues() {
        properties.getSubmit().setAllowResubmitWhenFinal(true);
        when(taskRepository.findByBizKey("k4"))
                .thenReturn(Optional.of(persisted("old", "k4", TaskStatus.FAILED)));
        when(taskRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(dispatcher.enqueueFromPending(any())).thenReturn(EnqueueOutcome.ENQUEUED);

        SubmitResponse resp = service.submit(req("k4"));

        assertNotEquals("old", resp.getTaskId());
        assertEquals(TaskStatus.QUEUED, resp.getStatus());
    }

    @Test
    void backpressureReject_throwsQueueFull() {
        when(taskRepository.findByBizKey("k5")).thenReturn(Optional.empty());
        when(taskRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(dispatcher.enqueueFromPending(any())).thenReturn(EnqueueOutcome.REJECTED);

        BusinessException ex = assertThrows(BusinessException.class, () -> service.submit(req("k5")));

        assertEquals(ErrorCode.QUEUE_FULL, ex.getCode());
        assertNotNull(ex.getTaskId());
    }

    @Test
    void backpressurePersistOnly_returnsPending() {
        properties.getQueue().setBackpressure("PERSIST_ONLY");
        when(taskRepository.findByBizKey("k6")).thenReturn(Optional.empty());
        when(taskRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(dispatcher.enqueueFromPending(any())).thenReturn(EnqueueOutcome.REJECTED);

        SubmitResponse resp = service.submit(req("k6"));

        assertEquals(TaskStatus.PENDING, resp.getStatus());
        assertNotNull(resp.getTaskId());
    }

    @Test
    void toctouConflict_fallsBackToExistingWinner() {
        when(taskRepository.findByBizKey("k7"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(persisted("id-7", "k7", TaskStatus.QUEUED)));
        when(taskRepository.save(any())).thenThrow(new DataIntegrityViolationException("duplicate key"));

        SubmitResponse resp = service.submit(req("k7"));

        assertEquals("id-7", resp.getTaskId());
        assertEquals(TaskStatus.QUEUED, resp.getStatus());
        verify(dispatcher, never()).enqueueFromPending(any());
    }

    @Test
    void gateClosed_throwsServiceUnavailableBeforeTouchingRepo() {
        acceptanceGate.shutdown();

        BusinessException ex = assertThrows(BusinessException.class, () -> service.submit(req("k8")));

        assertEquals(ErrorCode.SERVICE_SHUTTING_DOWN, ex.getCode());
        verify(taskRepository, never()).findByBizKey(any());
        verify(dispatcher, never()).enqueueFromPending(any());
    }
}