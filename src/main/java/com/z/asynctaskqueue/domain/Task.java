package com.z.asynctaskqueue.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

@Entity
@Table(name = "task")
public class Task implements PriorityAware {

    @Id
    @Column(name = "task_id", length = 64)
    private String taskId;

    @Column(name = "biz_key", nullable = false)
    private String bizKey;

    @Column(name = "task_type", nullable = false, length = 64)
    private String taskType;

    @Column(name = "priority", nullable = false)
    private int priority = 5;

    @Lob
    @Column(name = "payload")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private TaskStatus status;

    @Column(name = "fail_reason", length = 1024)
    private String failReason;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Column(name = "max_retry", nullable = false)
    private int maxRetry = 3;

    @Column(name = "next_retry_at")
    private LocalDateTime nextRetryAt;

    @Column(name = "heartbeat_at")
    private LocalDateTime heartbeatAt;

    @Column(name = "enqueued_at")
    private LocalDateTime enqueuedAt;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @Column(name = "finished_at")
    private LocalDateTime finishedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public Task() {
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getBizKey() { return bizKey; }
    public void setBizKey(String bizKey) { this.bizKey = bizKey; }
    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    @Override public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public String getFailReason() { return failReason; }
    public void setFailReason(String failReason) { this.failReason = failReason; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public int getMaxRetry() { return maxRetry; }
    public void setMaxRetry(int maxRetry) { this.maxRetry = maxRetry; }
    public LocalDateTime getNextRetryAt() { return nextRetryAt; }
    public void setNextRetryAt(LocalDateTime nextRetryAt) { this.nextRetryAt = nextRetryAt; }
    public LocalDateTime getHeartbeatAt() { return heartbeatAt; }
    public void setHeartbeatAt(LocalDateTime heartbeatAt) { this.heartbeatAt = heartbeatAt; }
    public LocalDateTime getEnqueuedAt() { return enqueuedAt; }
    public void setEnqueuedAt(LocalDateTime enqueuedAt) { this.enqueuedAt = enqueuedAt; }
    public LocalDateTime getStartedAt() { return startedAt; }
    public void setStartedAt(LocalDateTime startedAt) { this.startedAt = startedAt; }
    public LocalDateTime getFinishedAt() { return finishedAt; }
    public void setFinishedAt(LocalDateTime finishedAt) { this.finishedAt = finishedAt; }
    @Override public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}