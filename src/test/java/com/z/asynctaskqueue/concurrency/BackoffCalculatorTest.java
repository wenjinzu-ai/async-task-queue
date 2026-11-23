package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackoffCalculatorTest {

    private AsyncTaskProperties props(long base, long max, boolean jitter) {
        AsyncTaskProperties p = new AsyncTaskProperties();
        p.getRetry().setBaseBackoffMs(base);
        p.getRetry().setMaxBackoffMs(max);
        p.getRetry().setJitter(jitter);
        return p;
    }

    @Test
    void noJitter_firstAttemptEqualsBase() {
        BackoffCalculator calc = new BackoffCalculator(props(1000, 60000, false));
        assertEquals(1000, calc.computeBackoffMs(1));
    }

    @Test
    void noJitter_exponentialGrowth() {
        BackoffCalculator calc = new BackoffCalculator(props(1000, 60000, false));
        assertEquals(1000, calc.computeBackoffMs(1));
        assertEquals(2000, calc.computeBackoffMs(2));
        assertEquals(4000, calc.computeBackoffMs(3));
        assertEquals(8000, calc.computeBackoffMs(4));
    }

    @Test
    void capsAtMaxBackoff() {
        BackoffCalculator calc = new BackoffCalculator(props(1000, 5000, false));
        assertEquals(5000, calc.computeBackoffMs(4));
        assertEquals(5000, calc.computeBackoffMs(10));
    }

    @Test
    void jitterStaysWithinExpectedBand() {
        BackoffCalculator calc = new BackoffCalculator(props(1000, 60000, true), new Random(42));
        for (int i = 0; i < 1000; i++) {
            long v = calc.computeBackoffMs(1);
            assertTrue(v >= 800 && v <= 1200, "jitter out of [800,1200]: " + v);
        }
    }

    @Test
    void retryCountBelowOneTreatedAsFirstAttempt() {
        BackoffCalculator calc = new BackoffCalculator(props(1000, 60000, false));
        assertEquals(1000, calc.computeBackoffMs(0));
        assertEquals(1000, calc.computeBackoffMs(-5));
    }

    @Test
    void nextRetryAtIsInTheFuture() {
        BackoffCalculator calc = new BackoffCalculator(props(1000, 60000, false));
        assertTrue(calc.nextRetryAt(2).isAfter(LocalDateTime.now()));
    }
}