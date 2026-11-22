package com.z.asynctaskqueue.api;

import com.z.asynctaskqueue.api.dto.SubmitResponse;
import com.z.asynctaskqueue.domain.TaskStatus;
import com.z.asynctaskqueue.domain.exception.BusinessException;
import com.z.asynctaskqueue.domain.exception.ErrorCode;
import com.z.asynctaskqueue.service.TaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class TaskControllerTest {

    @Mock
    private TaskService taskService;

    private MockMvc mockMvc;

    private static final String VALID_BODY =
            "{\"bizKey\":\"biz-1\",\"taskType\":\"IMAGE\",\"priority\":5,\"payload\":\"{}\"}";
    private static final String MISSING_BIZ_KEY_BODY =
            "{\"taskType\":\"IMAGE\",\"priority\":5,\"payload\":\"{}\"}";

    @BeforeEach
    void setUp() {
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        mockMvc = MockMvcBuilders.standaloneSetup(new TaskController(taskService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setValidator(validator)
                .build();
    }

    @Test
    void submit_returns200WithTaskId() throws Exception {
        when(taskService.submit(any())).thenReturn(SubmitResponse.of("task-1", TaskStatus.QUEUED));

        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.taskId").value("task-1"))
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void submit_missingBizKey_returns400() throws Exception {
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(MISSING_BIZ_KEY_BODY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("PARAM_INVALID"));
    }

    @Test
    void submit_queueFull_returns429WithRetryAfter() throws Exception {
        when(taskService.submit(any()))
                .thenThrow(new BusinessException(ErrorCode.QUEUE_FULL, "queue full", "task-2"));

        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"))
                .andExpect(jsonPath("$.code").value("QUEUE_FULL"))
                .andExpect(jsonPath("$.taskId").value("task-2"));
    }

    @Test
    void submit_alreadyFinalized_returns409() throws Exception {
        when(taskService.submit(any()))
                .thenThrow(new BusinessException(ErrorCode.TASK_ALREADY_FINALIZED, "done", "task-3"));

        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("TASK_ALREADY_FINALIZED"));
    }
}