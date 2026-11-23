package com.z.asynctaskqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class AsyncTaskQueueApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncTaskQueueApplication.class, args);
    }

}