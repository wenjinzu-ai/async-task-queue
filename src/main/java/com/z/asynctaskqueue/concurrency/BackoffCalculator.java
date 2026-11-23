package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.config.AsyncTaskProperties;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class BackoffCalculator {

    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final boolean jitter;
    private final Random random;

    public BackoffCalculator(AsyncTaskProperties properties) {
        this(properties, new Random());
    }

    BackoffCalculator(AsyncTaskProperties properties, Random random) {
        AsyncTaskProperties.Retry retry = properties.getRetry();
        this.baseBackoffMs = Math.max(1, retry.getBaseBackoffMs());
        this.maxBackoffMs = Math.max(this.baseBackoffMs, retry.getMaxBackoffMs());
        this.jitter = retry.isJitter();
        this.random = random;
    }

    public long computeBackoffMs(int retryCount) {
        int attempt = Math.max(1, retryCount);
        long raw = baseBackoffMs;
        for (int i = 1; i < attempt && raw < maxBackoffMs; i++) {
            if (raw > Long.MAX_VALUE / 2) {
                raw = maxBackoffMs;
                break;
            }
            raw = Math.min(maxBackoffMs, raw * 2);
        }
        if (jitter) {
            double factor = 0.8 + random.nextDouble() * 0.4;
            raw = (long) (raw * factor);
        }
        return Math.max(1, raw);
    }

    public LocalDateTime nextRetryAt(int retryCount) {
        return LocalDateTime.now().plus(computeBackoffMs(retryCount), ChronoUnit.MILLIS);
    }
}