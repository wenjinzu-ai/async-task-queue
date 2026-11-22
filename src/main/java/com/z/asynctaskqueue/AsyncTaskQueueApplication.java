package com.z.asynctaskqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AsyncTaskQueueApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncTaskQueueApplication.class, args);
    }

}