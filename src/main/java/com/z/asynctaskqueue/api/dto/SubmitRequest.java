package com.z.asynctaskqueue.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class SubmitRequest {

    @NotBlank
    private String bizKey;

    @NotBlank
    private String taskType;

    @Min(0)
    @Max(9)
    private int priority = 5;

    @NotBlank
    private String payload;

    public String getBizKey() { return bizKey; }
    public void setBizKey(String bizKey) { this.bizKey = bizKey; }
    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}