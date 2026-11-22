package com.z.asynctaskqueue.repository;

import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class GuardConcurrencyTest {

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(transactionManager);
    }

    private Task newTask(String taskId, TaskStatus status) {
        Task t = new Task();
        t.setTaskId(taskId);
        t.setBizKey("biz-" + taskId);
        t.setTaskType("IMAGE");
        t.setPriority(5);
        t.setPayload("{}");
        t.setStatus(status);
        t.setRetryCount(0);
        t.setMaxRetry(3);
        LocalDateTime now = LocalDateTime.now();
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        return t;
    }

    private Task saveTask(Task task) {
        return tx.execute(s -> taskRepository.save(task));
    }

    private Task reload(String taskId) {
        return tx.execute(s -> taskRepository.findById(taskId).orElseThrow());
    }

    private int[] raceConcurrently(int threadCount, GuardAction action) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        AtomicIntegerArray results = new AtomicIntegerArray(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    int r = tx.execute(s -> action.run());
                    results.set(idx, r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "threads did not finish in time");
        pool.shutdownNow();
        int[] out = new int[threadCount];
        for (int i = 0; i < threadCount; i++) out[i] = results.get(i);
        return out;
    }

    private int sum(int[] arr) {
        int s = 0;
        for (int v : arr) s += v;
        return s;
    }

    private List<Integer> nonZero(int[] arr) {
        List<Integer> nz = new ArrayList<>();
        for (int i = 0; i < arr.length; i++) if (arr[i] != 0) nz.add(i);
        return nz;
    }

    @FunctionalInterface
    interface GuardAction {
        int run();
    }

    @Test
    void markQueuedFromPending_onlyOneSucceeds() throws InterruptedException {
        saveTask(newTask("g-pending", TaskStatus.PENDING));
        LocalDateTime now = LocalDateTime.now();

        int[] res = raceConcurrently(2, () -> taskRepository.markQueuedFromPending("g-pending", now));

        assertEquals(1, sum(res), "exactly one thread should win");
        assertEquals(1, nonZero(res).size());
        assertEquals(TaskStatus.QUEUED, reload("g-pending").getStatus());
    }

    @Test
    void markProcessing_duplicateQueueReferenceOnlyOneExecutes() throws InterruptedException {
        saveTask(newTask("g-queued", TaskStatus.QUEUED));
        LocalDateTime now = LocalDateTime.now();

        int[] res = raceConcurrently(2, () -> taskRepository.markProcessing("g-queued", now));

        assertEquals(1, sum(res), "only one of the duplicate references may execute");
        assertEquals(TaskStatus.PROCESSING, reload("g-queued").getStatus());
    }

    @Test
    void markQueuedFromRetryWaiting_dueOnly_oneSucceeds() throws InterruptedException {
        Task t = newTask("g-retry", TaskStatus.RETRY_WAITING);
        t.setNextRetryAt(LocalDateTime.now().minusSeconds(60));
        saveTask(t);
        LocalDateTime now = LocalDateTime.now();

        int[] res = raceConcurrently(2, () -> taskRepository.markQueuedFromRetryWaiting("g-retry", now, now));

        assertEquals(1, sum(res));
        assertEquals(TaskStatus.QUEUED, reload("g-retry").getStatus());
    }

    @Test
    void markQueuedFromRetryWaiting_notDue_noneSucceeds() {
        Task t = newTask("g-retry-future", TaskStatus.RETRY_WAITING);
        t.setNextRetryAt(LocalDateTime.now().plusSeconds(60));
        saveTask(t);
        LocalDateTime now = LocalDateTime.now();

        int r = tx.execute(s -> taskRepository.markQueuedFromRetryWaiting("g-retry-future", now, now));

        assertEquals(0, r, "not-yet-due retry must not be released");
        assertEquals(TaskStatus.RETRY_WAITING, reload("g-retry-future").getStatus());
    }

    @Test
    void recoverHeartbeatTimeout_onlyOneRecovers_andCountsOnce() throws InterruptedException {
        Task t = newTask("g-hb", TaskStatus.PROCESSING);
        t.setHeartbeatAt(LocalDateTime.now().minusSeconds(300));
        saveTask(t);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime threshold = LocalDateTime.now().minusSeconds(120);

        int[] res = raceConcurrently(2, () -> taskRepository.recoverHeartbeatTimeout("g-hb", now, threshold));

        assertEquals(1, sum(res), "only one thread recovers the timed-out task");
        Task reloaded = reload("g-hb");
        assertEquals(TaskStatus.RETRY_WAITING, reloaded.getStatus());
        assertEquals(1, reloaded.getRetryCount(), "retry_count incremented exactly once");
        assertEquals("heartbeat_timeout", reloaded.getFailReason());
    }

    @Test
    void recoverHeartbeatTimeout_heartbeatFresh_notRecovered() {
        Task t = newTask("g-hb-fresh", TaskStatus.PROCESSING);
        t.setHeartbeatAt(LocalDateTime.now());
        saveTask(t);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime threshold = LocalDateTime.now().minusSeconds(120);

        int r = tx.execute(s -> taskRepository.recoverHeartbeatTimeout("g-hb-fresh", now, threshold));

        assertEquals(0, r, "task with fresh heartbeat must not be recovered (no false kill)");
        assertEquals(TaskStatus.PROCESSING, reload("g-hb-fresh").getStatus());
    }

    @Test
    void markRetryWaiting_incrementsOnceEvenUnderConcurrency() throws InterruptedException {
        Task t = newTask("g-retry-count", TaskStatus.PROCESSING);
        saveTask(t);
        LocalDateTime next = LocalDateTime.now().plusSeconds(5);

        int[] res = raceConcurrently(2, () -> taskRepository.markRetryWaiting("g-retry-count", "err", next));

        assertEquals(1, sum(res), "only one transition may succeed");
        assertEquals(1, reload("g-retry-count").getRetryCount(), "retry_count incremented exactly once, no double count");
    }
}