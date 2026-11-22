package com.z.asynctaskqueue.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

@Entity
@Table(name = "task_result")
public class TaskResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Lob
    @Column(name = "result_data")
    private String resultData;

    @Column(name = "success", nullable = false)
    private boolean success;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    public TaskResult() {
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getResultData() { return resultData; }
    public void setResultData(String resultData) { this.resultData = resultData; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}