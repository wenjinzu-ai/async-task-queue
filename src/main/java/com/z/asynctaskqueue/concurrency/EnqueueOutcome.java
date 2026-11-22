package com.z.asynctaskqueue.concurrency;

public enum EnqueueOutcome {
    ENQUEUED,
    ALREADY_QUEUED,
    REJECTED
}