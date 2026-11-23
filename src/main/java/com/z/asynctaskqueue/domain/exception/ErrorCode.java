package com.z.asynctaskqueue.domain.exception;

import org.springframework.http.HttpStatus;

public enum ErrorCode {

    QUEUE_FULL(HttpStatus.TOO_MANY_REQUESTS),
    SERVICE_SHUTTING_DOWN(HttpStatus.SERVICE_UNAVAILABLE),
    TASK_ALREADY_FINALIZED(HttpStatus.CONFLICT),
    TASK_STATE_CONFLICT(HttpStatus.CONFLICT),
    TASK_NOT_FOUND(HttpStatus.NOT_FOUND),
    PARAM_INVALID(HttpStatus.BAD_REQUEST),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR);

    private final HttpStatus httpStatus;

    ErrorCode(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}