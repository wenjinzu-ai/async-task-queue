package com.z.asynctaskqueue.repository;

import com.z.asynctaskqueue.domain.TaskResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.Optional;

public interface TaskResultRepository extends JpaRepository<TaskResult, Long> {
    Optional<TaskResult> findByTaskId(String taskId);

    void deleteByTaskIdIn(Collection<String> taskIds);
}