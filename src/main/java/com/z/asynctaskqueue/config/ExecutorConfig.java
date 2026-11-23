package com.z.asynctaskqueue.config;

import com.z.asynctaskqueue.concurrency.BackoffCalculator;
import com.z.asynctaskqueue.concurrency.BoundedPriorityBlockingQueue;
import com.z.asynctaskqueue.concurrency.TaskDispatcher;
import com.z.asynctaskqueue.concurrency.TaskExecutor;
import com.z.asynctaskqueue.concurrency.TaskFailureHandler;
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
        int initial = properties.getQueue().getCapacity();
        if (initial <= 0) {
            initial = properties.getExecutor().getCorePoolSize() * properties.getQueue().getCoefficient();
        }
        if (initial <= 0) {
            initial = 1;
        }
        int max = Math.max(initial, properties.getQueue().getMaxCapacity());
        return new BoundedPriorityBlockingQueue<>(initial, max);
    }

    @Bean
    public TaskHandler taskHandler(TaskRepository taskRepository,
                                   TaskResultRepository taskResultRepository,
                                   AsyncTaskProperties properties,
                                   PlatformTransactionManager transactionManager) {
        return new TaskHandler(taskRepository, taskResultRepository, properties, transactionManager);
    }

    @Bean
    public TaskDispatcher taskDispatcher(BoundedPriorityBlockingQueue<Task> taskQueue,
                                         TaskRepository taskRepository,
                                         AsyncTaskProperties properties,
                                         PlatformTransactionManager transactionManager) {
        return new TaskDispatcher(taskQueue, taskRepository, properties, transactionManager);
    }

    @Bean
    public BackoffCalculator backoffCalculator(AsyncTaskProperties properties) {
        return new BackoffCalculator(properties);
    }

    @Bean
    public TaskFailureHandler taskFailureHandler(TaskRepository taskRepository,
                                                 BackoffCalculator backoffCalculator,
                                                 AsyncTaskProperties properties,
                                                 PlatformTransactionManager transactionManager) {
        return new TaskFailureHandler(taskRepository, backoffCalculator, properties, transactionManager);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public TaskExecutor taskExecutor(BoundedPriorityBlockingQueue<Task> taskQueue,
                                     TaskHandler taskHandler,
                                     TaskRepository taskRepository,
                                     TaskFailureHandler taskFailureHandler,
                                     AsyncTaskProperties properties,
                                     PlatformTransactionManager transactionManager) {
        return new TaskExecutor(taskQueue, taskHandler, taskRepository, taskFailureHandler,
                properties, transactionManager);
    }
}