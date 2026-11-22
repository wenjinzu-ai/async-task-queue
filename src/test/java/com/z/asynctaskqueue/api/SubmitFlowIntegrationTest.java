package com.z.asynctaskqueue.api;

import com.z.asynctaskqueue.api.dto.SubmitRequest;
import com.z.asynctaskqueue.api.dto.SubmitResponse;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.repository.TaskRepository;
import com.z.asynctaskqueue.service.TaskService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = {
        "async-task.handler.simulate-min-ms=40",
        "async-task.handler.simulate-max-ms=40",
        "async-task.handler.simulate-seed=7",
        "async-task.heartbeat.interval-ms=15"
})
class SubmitFlowIntegrationTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private SubmitRequest request(String bizKey) {
        SubmitRequest r = new SubmitRequest();
        r.setBizKey(bizKey);
        r.setTaskType("IMAGE");
        r.setPriority(5);
        r.setPayload("{\"hello\":\"world\"}");
        return r;
    }

    private com.z.asynctaskqueue.domain.Task waitForStatus(String id, TaskStatus expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        com.z.asynctaskqueue.domain.Task t;
        do {
            t = tx().execute(s -> taskRepository.findById(id).orElseThrow());
            if (t.getStatus() == expected) {
                return t;
            }
            Thread.sleep(20);
        } while (System.currentTimeMillis() < deadline);
        return t;
    }

    @Test
    void submit_newTask_reachesSuccessThroughFullPipeline() throws Exception {
        SubmitResponse resp = taskService.submit(request("flow-new"));

        assertNotNull(resp.getTaskId());
        assertEquals(TaskStatus.QUEUED, resp.getStatus());

        com.z.asynctaskqueue.domain.Task done = waitForStatus(resp.getTaskId(), TaskStatus.SUCCESS, 5000);
        assertEquals(TaskStatus.SUCCESS, done.getStatus(),
                "a freshly submitted task must flow PENDING->QUEUED->PROCESSING->SUCCESS end to end");
    }

    @Test
    void submit_sameBizKeyTwice_returnsSameTaskIdWithoutDoubleRun() throws Exception {
        SubmitResponse first = taskService.submit(request("flow-idem"));
        SubmitResponse second = taskService.submit(request("flow-idem"));

        assertEquals(first.getTaskId(), second.getTaskId(),
                "an in-flight task with the same bizKey must return the original taskId (idempotency)");

        com.z.asynctaskqueue.domain.Task done = waitForStatus(first.getTaskId(), TaskStatus.SUCCESS, 5000);
        assertEquals(TaskStatus.SUCCESS, done.getStatus());
        assertEquals(0, done.getRetryCount(), "idempotent re-submit must not create a second run or inflate retry count");
    }

    @Test
    void submit_distinctBizKeys_produceDistinctTaskIds() {
        SubmitResponse a = taskService.submit(request("flow-a"));
        SubmitResponse b = taskService.submit(request("flow-b"));

        assertNotEquals(a.getTaskId(), b.getTaskId());
    }
}