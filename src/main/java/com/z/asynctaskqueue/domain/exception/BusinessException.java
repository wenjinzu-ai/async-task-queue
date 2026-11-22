package com.z.asynctaskqueue.domain.exception;

public class BusinessException extends RuntimeException {

    private final ErrorCode code;
    private final String taskId;

    public BusinessException(ErrorCode code, String message) {
        this(code, message, null);
    }

    public BusinessException(ErrorCode code, String message, String taskId) {
        super(message);
        this.code = code;
        this.taskId = taskId;
    }

    public ErrorCode getCode() {
        return code;
    }

    public String getTaskId() {
        return taskId;
    }
}