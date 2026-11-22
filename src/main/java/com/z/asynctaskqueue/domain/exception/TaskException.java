package com.z.asynctaskqueue.domain.exception;

public class TaskException extends RuntimeException {

    private final boolean retryable;

    public TaskException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public TaskException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}