package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.domain.FailureCategory;
import com.z.asynctaskqueue.domain.exception.TaskException;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;

public final class FailureClassifier {

    private static final List<Class<?>> TRANSIENT_SYSTEM = List.of(
            SocketTimeoutException.class,
            SocketException.class
    );

    private FailureClassifier() {
    }

    public static FailureCategory classify(Throwable t) {
        Throwable cursor = t;
        while (cursor != null) {
            if (cursor instanceof TaskException taskException) {
                return taskException.isRetryable()
                        ? FailureCategory.BUSINESS_RETRYABLE
                        : FailureCategory.BUSINESS_NON_RETRYABLE;
            }
            for (Class<?> type : TRANSIENT_SYSTEM) {
                if (type.isInstance(cursor)) {
                    return FailureCategory.SYSTEM_RECOVERABLE;
                }
            }
            if (cursor instanceof Error) {
                return FailureCategory.SYSTEM_UNRECOVERABLE;
            }
            cursor = cursor.getCause();
        }
        return FailureCategory.SYSTEM_UNRECOVERABLE;
    }

    public static boolean isRetryable(FailureCategory category) {
        return category == FailureCategory.BUSINESS_RETRYABLE
                || category == FailureCategory.SYSTEM_RECOVERABLE;
    }
}