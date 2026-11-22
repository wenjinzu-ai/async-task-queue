package com.z.asynctaskqueue.repository;

import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.domain.TaskStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface TaskRepository extends JpaRepository<Task, String> {

    Optional<Task> findByBizKey(String bizKey);

    Page<Task> findByStatus(TaskStatus status, Pageable pageable);

    List<Task> findByStatus(TaskStatus status);

    List<Task> findByStatusAndNextRetryAtBefore(TaskStatus status, LocalDateTime before);

    List<Task> findByStatusAndHeartbeatAtBefore(TaskStatus status, LocalDateTime before);

    @Query("SELECT t.status, COUNT(t) FROM Task t GROUP BY t.status")
    List<Object[]> countGroupByStatus();

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.QUEUED, t.enqueuedAt = :enqueuedAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PENDING")
    int markQueuedFromPending(@Param("taskId") String taskId, @Param("enqueuedAt") LocalDateTime enqueuedAt);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.QUEUED, t.enqueuedAt = :enqueuedAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.RETRY_WAITING AND t.nextRetryAt <= :now")
    int markQueuedFromRetryWaiting(@Param("taskId") String taskId, @Param("enqueuedAt") LocalDateTime enqueuedAt, @Param("now") LocalDateTime now);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING, t.startedAt = :startedAt, t.heartbeatAt = :startedAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.QUEUED")
    int markProcessing(@Param("taskId") String taskId, @Param("startedAt") LocalDateTime startedAt);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.SUCCESS, t.finishedAt = :finishedAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING")
    int markSuccess(@Param("taskId") String taskId, @Param("finishedAt") LocalDateTime finishedAt);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.RETRY_WAITING, " +
           "t.retryCount = t.retryCount + 1, t.failReason = :failReason, t.nextRetryAt = :nextRetryAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING")
    int markRetryWaiting(@Param("taskId") String taskId, @Param("failReason") String failReason, @Param("nextRetryAt") LocalDateTime nextRetryAt);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.DEAD_LETTER, " +
           "t.retryCount = t.retryCount + 1, t.failReason = :failReason, t.finishedAt = :finishedAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING")
    int markDeadLetter(@Param("taskId") String taskId, @Param("failReason") String failReason, @Param("finishedAt") LocalDateTime finishedAt);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.FAILED, t.failReason = :failReason, t.finishedAt = :finishedAt " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING")
    int markFailed(@Param("taskId") String taskId, @Param("failReason") String failReason, @Param("finishedAt") LocalDateTime finishedAt);

    @Modifying
    @Query("UPDATE Task t SET t.heartbeatAt = :now " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING")
    int refreshHeartbeat(@Param("taskId") String taskId, @Param("now") LocalDateTime now);

    @Modifying
    @Query("UPDATE Task t SET t.status = com.z.asynctaskqueue.domain.TaskStatus.RETRY_WAITING, " +
           "t.failReason = 'heartbeat_timeout', t.retryCount = t.retryCount + 1, t.nextRetryAt = :now " +
           "WHERE t.taskId = :taskId AND t.status = com.z.asynctaskqueue.domain.TaskStatus.PROCESSING AND t.heartbeatAt < :threshold")
    int recoverHeartbeatTimeout(@Param("taskId") String taskId, @Param("now") LocalDateTime now, @Param("threshold") LocalDateTime threshold);
}