package com.z.asynctaskqueue.api.dto;

import com.z.asynctaskqueue.domain.TaskStatus;

public class SubmitResponse {

    private String taskId;
    private TaskStatus status;

    public SubmitResponse() {
    }

    public SubmitResponse(String taskId, TaskStatus status) {
        this.taskId = taskId;
        this.status = status;
    }

    public static SubmitResponse of(String taskId, TaskStatus status) {
        return new SubmitResponse(taskId, status);
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
}