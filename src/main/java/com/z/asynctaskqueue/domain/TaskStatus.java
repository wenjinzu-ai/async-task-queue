package com.z.asynctaskqueue.domain;

public enum TaskStatus {
    PENDING,
    QUEUED,
    PROCESSING,
    RETRY_WAITING,
    SUCCESS,
    FAILED,
    DEAD_LETTER
}