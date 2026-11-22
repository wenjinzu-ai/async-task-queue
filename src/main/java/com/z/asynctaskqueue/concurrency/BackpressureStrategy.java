package com.z.asynctaskqueue.concurrency;

public enum BackpressureStrategy {

    REJECT,
    PERSIST_ONLY,
    BLOCK;

    public static BackpressureStrategy from(String name) {
        if (name == null || name.isBlank()) {
            return REJECT;
        }
        try {
            return BackpressureStrategy.valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return REJECT;
        }
    }
}