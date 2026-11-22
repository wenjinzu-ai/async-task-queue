package com.z.asynctaskqueue.domain;

public enum FailureCategory {
    BUSINESS_RETRYABLE,
    BUSINESS_NON_RETRYABLE,
    SYSTEM_RECOVERABLE,
    SYSTEM_UNRECOVERABLE
}