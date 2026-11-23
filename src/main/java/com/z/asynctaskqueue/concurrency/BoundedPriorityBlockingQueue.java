package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.domain.PriorityAware;

import java.time.LocalDateTime;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedPriorityBlockingQueue<E extends PriorityAware> implements BlockingQueue<E> {

    private final Object[] heap;
    private final int maxCapacity;
    private volatile int capacity;
    private int size;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public BoundedPriorityBlockingQueue(int capacity) {
        this(capacity, capacity);
    }

    public BoundedPriorityBlockingQueue(int initialCapacity, int maxCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initial capacity must be positive: " + initialCapacity);
        }
        if (maxCapacity < initialCapacity) {
            throw new IllegalArgumentException("max capacity must be >= initial capacity: " + maxCapacity);
        }
        this.maxCapacity = maxCapacity;
        this.capacity = initialCapacity;
        this.heap = new Object[maxCapacity];
    }

    public int currentCapacity() {
        return capacity;
    }

    public int maxCapacity() {
        return maxCapacity;
    }

    public boolean setCapacity(int newCapacity) {
        if (newCapacity <= 0 || newCapacity > maxCapacity) {
            return false;
        }
        lock.lock();
        try {
            int old = this.capacity;
            this.capacity = newCapacity;
            if (newCapacity > old) {
                notFull.signalAll();
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private int compare(E a, E b) {
        int cmp = Integer.compare(b.getPriority(), a.getPriority());
        if (cmp != 0) {
            return cmp;
        }
        LocalDateTime ca = a.getCreatedAt();
        LocalDateTime cb = b.getCreatedAt();
        if (ca == null && cb == null) {
            return 0;
        }
        if (ca == null) {
            return 1;
        }
        if (cb == null) {
            return -1;
        }
        return ca.compareTo(cb);
    }

    private void siftUp(int i) {
        E x = elementAt(i);
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            E p = elementAt(parent);
            if (compare(x, p) >= 0) {
                break;
            }
            heap[i] = p;
            i = parent;
        }
        heap[i] = x;
    }

    private void siftDown(int i) {
        E x = elementAt(i);
        int half = size >>> 1;
        while (i < half) {
            int child = (i << 1) + 1;
            E c = elementAt(child);
            int right = child + 1;
            if (right < size) {
                E r = elementAt(right);
                if (compare(r, c) < 0) {
                    child = right;
                    c = r;
                }
            }
            if (compare(x, c) <= 0) {
                break;
            }
            heap[i] = c;
            i = child;
        }
        heap[i] = x;
    }

    @SuppressWarnings("unchecked")
    private E elementAt(int i) {
        return (E) heap[i];
    }

    private void insert(E e) {
        heap[size] = e;
        siftUp(size);
        size++;
        notEmpty.signal();
    }

    private E extract() {
        E top = elementAt(0);
        size--;
        E last = elementAt(size);
        heap[size] = null;
        if (size > 0) {
            heap[0] = last;
            siftDown(0);
        }
        notFull.signal();
        return top;
    }

    private void checkNotNull(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
    }

    @Override
    public boolean offer(E e) {
        checkNotNull(e);
        lock.lock();
        try {
            if (size >= capacity) {
                return false;
            }
            insert(e);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(e);
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (size >= capacity) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            insert(e);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        checkNotNull(e);
        lock.lockInterruptibly();
        try {
            while (size >= capacity) {
                notFull.await();
            }
            insert(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (size == 0) {
                notEmpty.await();
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (size == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try {
            return size == 0 ? null : extract();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue full");
    }

    @Override
    public E remove() {
        E x = poll();
        if (x == null) {
            throw new NoSuchElementException();
        }
        return x;
    }

    @Override
    public E element() {
        E x = peek();
        if (x == null) {
            throw new NoSuchElementException();
        }
        return x;
    }

    @Override
    public E peek() {
        lock.lock();
        try {
            return size == 0 ? null : elementAt(0);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        lock.lock();
        try {
            return Math.max(0, capacity - size);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        if (c == this) {
            throw new IllegalArgumentException();
        }
        lock.lock();
        try {
            int n = 0;
            while (size > 0) {
                c.add(extract());
                n++;
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == this) {
            throw new IllegalArgumentException();
        }
        if (maxElements <= 0) {
            return 0;
        }
        lock.lock();
        try {
            int n = 0;
            while (n < maxElements && size > 0) {
                c.add(extract());
                n++;
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    private int indexOf(Object o) {
        for (int i = 0; i < size; i++) {
            if (o.equals(heap[i])) {
                return i;
            }
        }
        return -1;
    }

    private void removeAt(int i) {
        size--;
        if (i == size) {
            heap[size] = null;
            return;
        }
        E moved = elementAt(size);
        heap[size] = null;
        heap[i] = moved;
        siftDown(i);
        if (heap[i] == moved) {
            siftUp(i);
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }
        lock.lock();
        try {
            int i = indexOf(o);
            if (i == -1) {
                return false;
            }
            removeAt(i);
            notFull.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        lock.lock();
        try {
            return indexOf(o) != -1;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object[] toArray() {
        lock.lock();
        try {
            return Arrays.copyOf(heap, size);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        final Object[] snap;
        lock.lock();
        try {
            snap = Arrays.copyOf(heap, size, Object[].class);
        } finally {
            lock.unlock();
        }
        if (a.length < snap.length) {
            @SuppressWarnings("unchecked")
            T[] r = (T[]) Arrays.copyOf(snap, snap.length, a.getClass());
            return r;
        }
        System.arraycopy(snap, 0, a, 0, snap.length);
        if (a.length > snap.length) {
            a[snap.length] = null;
        }
        return a;
    }

    @Override
    public Iterator<E> iterator() {
        List<E> snap = new ArrayList<>();
        lock.lock();
        try {
            for (int i = 0; i < size; i++) {
                snap.add(elementAt(i));
            }
        } finally {
            lock.unlock();
        }
        return new AbstractCollection<E>() {
            @Override
            public Iterator<E> iterator() {
                return snap.iterator();
            }

            @Override
            public int size() {
                return snap.size();
            }
        }.iterator();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        for (E e : c) {
            add(e);
        }
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (Object o : c) {
            modified |= remove(o);
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        List<E> snap = new ArrayList<>();
        lock.lock();
        try {
            for (int i = 0; i < size; i++) {
                snap.add(elementAt(i));
            }
        } finally {
            lock.unlock();
        }
        boolean modified = false;
        for (E e : snap) {
            if (!c.contains(e)) {
                modified |= remove(e);
            }
        }
        return modified;
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            for (int i = 0; i < size; i++) {
                heap[i] = null;
            }
            size = 0;
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(heap[i]);
            }
            return sb.append("]").toString();
        } finally {
            lock.unlock();
        }
    }
}