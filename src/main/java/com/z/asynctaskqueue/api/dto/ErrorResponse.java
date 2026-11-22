package com.z.asynctaskqueue.api.dto;

import com.z.asynctaskqueue.domain.exception.ErrorCode;

import java.time.Instant;

public class ErrorResponse {

    private String code;
    private String message;
    private String taskId;
    private Instant timestamp;

    public ErrorResponse() {
    }

    public ErrorResponse(String code, String message, String taskId, Instant timestamp) {
        this.code = code;
        this.message = message;
        this.taskId = taskId;
        this.timestamp = timestamp;
    }

    public static ErrorResponse of(ErrorCode code, String message, String taskId) {
        return new ErrorResponse(code.name(), message, taskId, Instant.now());
    }

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}