package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.domain.PriorityAware;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedPriorityBlockingQueueTest {

    static class Item implements PriorityAware {
        final String id;
        final int priority;
        final LocalDateTime createdAt;

        Item(String id, int priority, LocalDateTime createdAt) {
            this.id = id;
            this.priority = priority;
            this.createdAt = createdAt;
        }

        @Override public int getPriority() { return priority; }
        @Override public LocalDateTime getCreatedAt() { return createdAt; }
        @Override public String toString() { return id; }
    }

    private Item item(String id, int priority) {
        return new Item(id, priority, LocalDateTime.now());
    }

    @Test
    void concurrentProducersConsumers_noLossNoDuplicate() throws Exception {
        int producers = 4;
        int consumers = 3;
        int perProducer = 250;
        int total = producers * perProducer;
        BoundedPriorityBlockingQueue<Item> q = new BoundedPriorityBlockingQueue<>(128);
        Set<String> consumed = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicInteger consumedCount = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers + consumers);

        for (int p = 0; p < producers; p++) {
            final int pIdx = p;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        Item it = new Item("p" + pIdx + "-" + i, i % 5, LocalDateTime.now());
                        while (!q.offer(it, 1, TimeUnit.SECONDS)) {
                            // keep retrying under backpressure
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        for (int c = 0; c < consumers; c++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (consumedCount.get() < total) {
                        Item it = q.poll(50, TimeUnit.MILLISECONDS);
                        if (it != null) {
                            assertTrue(consumed.add(it.id), "duplicate element consumed: " + it.id);
                            consumedCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "producers/consumers did not finish in time");
        pool.shutdownNow();

        assertEquals(total, consumedCount.get(), "all produced elements must be consumed exactly once");
        assertEquals(total, consumed.size(), "no duplicates, no loss");
        assertEquals(0, q.size());
    }

    @Test
    void priorityOrder_highestFirst_thenFifoByCreatedAt() {
        BoundedPriorityBlockingQueue<Item> q = new BoundedPriorityBlockingQueue<>(10);
        LocalDateTime base = LocalDateTime.of(2026, 1, 1, 0, 0, 0);
        q.offer(new Item("a", 1, base));
        q.offer(new Item("b", 5, base.plusSeconds(1)));
        q.offer(new Item("c", 3, base.plusSeconds(2)));
        q.offer(new Item("d", 5, base.plusSeconds(3))); // same priority as b, later createdAt
        q.offer(new Item("e", 5, base));                 // same priority as b, earlier createdAt

        List<String> order = new ArrayList<>();
        Item it;
        while ((it = q.poll()) != null) {
            order.add(it.id);
        }

        // priority 5 first, ordered by createdAt asc: e(base) < b(+1s) < d(+3s), then c(3), then a(1)
        assertEquals(List.of("e", "b", "d", "c", "a"), order);
    }

    @Test
    void capacityBoundaries_fullOfferTimesOut_emptyPollTimesOut() throws Exception {
        BoundedPriorityBlockingQueue<Item> q = new BoundedPriorityBlockingQueue<>(2);
        assertTrue(q.offer(item("1", 1)));
        assertTrue(q.offer(item("2", 1)));
        assertEquals(2, q.size());

        boolean offered = q.offer(item("3", 1), 50, TimeUnit.MILLISECONDS);
        assertFalse(offered, "offer must time out (return false) when full -> backpressure signal");
        assertEquals(0, q.remainingCapacity());

        assertNotNull(q.poll());
        assertNotNull(q.poll());
        assertEquals(0, q.size());

        Item polled = q.poll(50, TimeUnit.MILLISECONDS);
        assertNull(polled, "poll must time out (return null) when empty");
    }

    @Test
    void takeBlocksWhenEmpty_unblocksOnOffer() throws Exception {
        BoundedPriorityBlockingQueue<Item> q = new BoundedPriorityBlockingQueue<>(2);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<Item> future = pool.submit(q::take);

        Thread.sleep(150);
        assertFalse(future.isDone(), "take() must block while queue is empty");

        q.offer(item("x", 9));
        Item got = future.get(1, TimeUnit.SECONDS);
        assertEquals("x", got.id);
        pool.shutdownNow();
    }

    @Test
    void drainTo_takesAllAndLeavesQueueEmpty() {
        BoundedPriorityBlockingQueue<Item> q = new BoundedPriorityBlockingQueue<>(10);
        for (int i = 0; i < 7; i++) {
            q.offer(item("n" + i, i));
        }
        List<Item> out = new ArrayList<>();
        int drained = q.drainTo(out);

        assertEquals(7, drained);
        assertEquals(7, out.size());
        assertEquals(0, q.size());
        Set<String> ids = new HashSet<>();
        for (Item it : out) ids.add(it.id);
        assertEquals(7, ids.size(), "drained snapshot must be complete and duplicate-free");
    }

    @Test
    void drainToWithMax_respectsLimit() {
        BoundedPriorityBlockingQueue<Item> q = new BoundedPriorityBlockingQueue<>(10);
        for (int i = 0; i < 5; i++) {
            q.offer(item("m" + i, i));
        }
        List<Item> out = new ArrayList<>();
        assertEquals(3, q.drainTo(out, 3));
        assertEquals(2, q.size());
        assertEquals(3, out.size());
    }
}