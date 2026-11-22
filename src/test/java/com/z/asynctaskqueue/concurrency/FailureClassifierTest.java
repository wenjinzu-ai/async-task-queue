package com.z.asynctaskqueue.concurrency;

import com.z.asynctaskqueue.domain.FailureCategory;
import com.z.asynctaskqueue.domain.exception.TaskException;
import org.junit.jupiter.api.Test;

import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureClassifierTest {

    @Test
    void businessRetryable_mapsToBusinessRetryable() {
        FailureCategory c = FailureClassifier.classify(new TaskException("model 500", true));
        assertEquals(FailureCategory.BUSINESS_RETRYABLE, c);
        assertTrue(FailureClassifier.isRetryable(c));
    }

    @Test
    void businessNonRetryable_mapsToBusinessNonRetryable() {
        FailureCategory c = FailureClassifier.classify(new TaskException("content rejected", false));
        assertEquals(FailureCategory.BUSINESS_NON_RETRYABLE, c);
        assertFalse(FailureClassifier.isRetryable(c));
    }

    @Test
    void transientSystemException_mapsToSystemRecoverable() {
        assertEquals(FailureCategory.SYSTEM_RECOVERABLE,
                FailureClassifier.classify(new SocketTimeoutException("read timed out")));
        assertEquals(FailureCategory.SYSTEM_RECOVERABLE,
                FailureClassifier.classify(new SocketException("connection reset")));
        assertTrue(FailureClassifier.isRetryable(FailureCategory.SYSTEM_RECOVERABLE));
    }

    @Test
    void unknownExceptionAndError_mapToSystemUnrecoverable() {
        FailureCategory c1 = FailureClassifier.classify(new RuntimeException("boom"));
        assertEquals(FailureCategory.SYSTEM_UNRECOVERABLE, c1);
        assertFalse(FailureClassifier.isRetryable(c1));

        FailureCategory c2 = FailureClassifier.classify(new AssertionError("jvm error"));
        assertEquals(FailureCategory.SYSTEM_UNRECOVERABLE, c2);
    }

    @Test
    void causeIsUnwrappedToFindTaskException() {
        Throwable wrapped = new RuntimeException("wrapper", new TaskException("inner retryable", true));
        assertEquals(FailureCategory.BUSINESS_RETRYABLE, FailureClassifier.classify(wrapped));
    }
}