package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskDispatcherTest {

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private PlatformTransactionManager transactionManager;

    private BoundedPriorityBlockingQueue<Task> queue;
    private AsyncTaskProperties properties;
    private TaskDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        queue = new BoundedPriorityBlockingQueue<>(2);
        properties = new AsyncTaskProperties();
        properties.getQueue().setOfferTimeoutMs(10);
        when(transactionManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
        dispatcher = new TaskDispatcher(queue, taskRepository, properties, transactionManager);
    }

    private Task task(String id) {
        Task t = new Task();
        t.setTaskId(id);
        t.setBizKey("b-" + id);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(TaskStatus.PENDING);
        t.setRetryCount(0);
        t.setMaxRetry(3);
        LocalDateTime now = LocalDateTime.now();
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return t;
    }

    @Test
    void enqueueSuccess_flipsQueuedAndOffers() {
        Task task = task("d1");
        when(taskRepository.markQueuedFromPending(eq("d1"), any())).thenReturn(1);

        assertEquals(EnqueueOutcome.ENQUEUED, dispatcher.enqueueFromPending(task));
        assertEquals(1, queue.size());
        verify(taskRepository, never()).revertToPending(any());
    }

    @Test
    void guardAlreadyQueued_returnsAlreadyQueuedWithoutOffer() {
        Task task = task("d2");
        when(taskRepository.markQueuedFromPending(eq("d2"), any())).thenReturn(0);

        assertEquals(EnqueueOutcome.ALREADY_QUEUED, dispatcher.enqueueFromPending(task));
        assertEquals(0, queue.size());
        verify(taskRepository, never()).revertToPending(any());
    }

    @Test
    void backpressure_revertsToPendingWithoutLosingTask() {
        queue.offer(task("fill-1"));
        queue.offer(task("fill-2"));

        Task task = task("d3");
        when(taskRepository.markQueuedFromPending(eq("d3"), any())).thenReturn(1);

        assertEquals(EnqueueOutcome.REJECTED, dispatcher.enqueueFromPending(task));
        verify(taskRepository).revertToPending("d3");
        assertEquals(2, queue.size(), "the rejected task must not land in the memory queue");
    }
}