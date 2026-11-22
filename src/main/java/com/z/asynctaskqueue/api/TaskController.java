package com.z.asynctaskqueue.api;

import com.z.asynctaskqueue.api.dto.SubmitRequest;
import com.z.asynctaskqueue.api.dto.SubmitResponse;
import com.z.asynctaskqueue.service.TaskService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public ResponseEntity<SubmitResponse> submit(@Valid @RequestBody SubmitRequest request) {
        return ResponseEntity.ok(taskService.submit(request));
    }
}