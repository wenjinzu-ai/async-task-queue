package com.z.asynctaskqueue.domain;

import java.time.LocalDateTime;

public interface PriorityAware {
    int getPriority();
    LocalDateTime getCreatedAt();
}