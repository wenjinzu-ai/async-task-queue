package com.z.asynctaskqueue.config;

import com.z.asynctaskqueue.concurrency.BoundedPriorityBlockingQueue;
import com.z.asynctaskqueue.concurrency.TaskExecutor;
import com.z.asynctaskqueue.concurrency.TaskHandler;
import com.z.asynctaskqueue.domain.Task;
import com.z.asynctaskqueue.repository.TaskRepository;
import com.z.asynctaskqueue.repository.TaskResultRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ExecutorConfig {

    @Bean
    public BoundedPriorityBlockingQueue<Task> taskQueue(AsyncTaskProperties properties) {
        int capacity = properties.getQueue().getCapacity();
        if (capacity <= 0) {
            capacity = properties.getExecutor().getCorePoolSize() * properties.getQueue().getCoefficient();
        }
        if (capacity <= 0) {
            capacity = 1;
        }
        return new BoundedPriorityBlockingQueue<>(capacity);
    }

    @Bean
    public TaskHandler taskHandler(TaskRepository taskRepository,
                                   TaskResultRepository taskResultRepository,
                                   AsyncTaskProperties properties,
                                   PlatformTransactionManager transactionManager) {
        return new TaskHandler(taskRepository, taskResultRepository, properties, transactionManager);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public TaskExecutor taskExecutor(BoundedPriorityBlockingQueue<Task> taskQueue,
                                     TaskHandler taskHandler,
                                     TaskRepository taskRepository,
                                     AsyncTaskProperties properties,
                                     PlatformTransactionManager transactionManager) {
        return new TaskExecutor(taskQueue, taskHandler, taskRepository, properties, transactionManager);
    }
}