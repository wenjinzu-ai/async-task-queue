package com.z.asynctaskqueue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "async-task")
public class AsyncTaskProperties {

    private Submit submit = new Submit();
    private Queue queue = new Queue();
    private Executor executor = new Executor();
    private Retry retry = new Retry();
    private Handler handler = new Handler();
    private Heartbeat heartbeat = new Heartbeat();
    private Cleanup cleanup = new Cleanup();
    private Shutdown shutdown = new Shutdown();

    public Submit getSubmit() { return submit; }
    public void setSubmit(Submit submit) { this.submit = submit; }
    public Queue getQueue() { return queue; }
    public void setQueue(Queue queue) { this.queue = queue; }
    public Executor getExecutor() { return executor; }
    public void setExecutor(Executor executor) { this.executor = executor; }
    public Retry getRetry() { return retry; }
    public void setRetry(Retry retry) { this.retry = retry; }
    public Handler getHandler() { return handler; }
    public void setHandler(Handler handler) { this.handler = handler; }
    public Heartbeat getHeartbeat() { return heartbeat; }
    public void setHeartbeat(Heartbeat heartbeat) { this.heartbeat = heartbeat; }
    public Cleanup getCleanup() { return cleanup; }
    public void setCleanup(Cleanup cleanup) { this.cleanup = cleanup; }
    public Shutdown getShutdown() { return shutdown; }
    public void setShutdown(Shutdown shutdown) { this.shutdown = shutdown; }

    public static class Submit {
        private boolean allowResubmitWhenFinal = false;
        public boolean isAllowResubmitWhenFinal() { return allowResubmitWhenFinal; }
        public void setAllowResubmitWhenFinal(boolean allowResubmitWhenFinal) { this.allowResubmitWhenFinal = allowResubmitWhenFinal; }
    }

    public static class Queue {
        private int capacity = 2000;
        private int coefficient = 100;
        private int minCapacity = 500;
        private int maxCapacity = 10000;
        private boolean adaptive = false;
        private String backpressure = "REJECT";
        private long offerTimeoutMs = 50;

        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public int getCoefficient() { return coefficient; }
        public void setCoefficient(int coefficient) { this.coefficient = coefficient; }
        public int getMinCapacity() { return minCapacity; }
        public void setMinCapacity(int minCapacity) { this.minCapacity = minCapacity; }
        public int getMaxCapacity() { return maxCapacity; }
        public void setMaxCapacity(int maxCapacity) { this.maxCapacity = maxCapacity; }
        public boolean isAdaptive() { return adaptive; }
        public void setAdaptive(boolean adaptive) { this.adaptive = adaptive; }
        public String getBackpressure() { return backpressure; }
        public void setBackpressure(String backpressure) { this.backpressure = backpressure; }
        public long getOfferTimeoutMs() { return offerTimeoutMs; }
        public void setOfferTimeoutMs(long offerTimeoutMs) { this.offerTimeoutMs = offerTimeoutMs; }
    }

    public static class Executor {
        private int corePoolSize = 8;
        private int maxPoolSize = 8;
        private String threadNamePrefix = "task-worker";

        public int getCorePoolSize() { return corePoolSize; }
        public void setCorePoolSize(int corePoolSize) { this.corePoolSize = corePoolSize; }
        public int getMaxPoolSize() { return maxPoolSize; }
        public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
        public String getThreadNamePrefix() { return threadNamePrefix; }
        public void setThreadNamePrefix(String threadNamePrefix) { this.threadNamePrefix = threadNamePrefix; }
    }

    public static class Retry {
        private int maxRetry = 3;
        private long baseBackoffMs = 1000;
        private long maxBackoffMs = 60000;
        private boolean jitter = true;
        private int schedulerPoolSize = 2;

        public int getMaxRetry() { return maxRetry; }
        public void setMaxRetry(int maxRetry) { this.maxRetry = maxRetry; }
        public long getBaseBackoffMs() { return baseBackoffMs; }
        public void setBaseBackoffMs(long baseBackoffMs) { this.baseBackoffMs = baseBackoffMs; }
        public long getMaxBackoffMs() { return maxBackoffMs; }
        public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
        public boolean isJitter() { return jitter; }
        public void setJitter(boolean jitter) { this.jitter = jitter; }
        public int getSchedulerPoolSize() { return schedulerPoolSize; }
        public void setSchedulerPoolSize(int schedulerPoolSize) { this.schedulerPoolSize = schedulerPoolSize; }
    }

    public static class Handler {
        private long callTimeoutMs = 90000;
        public long getCallTimeoutMs() { return callTimeoutMs; }
        public void setCallTimeoutMs(long callTimeoutMs) { this.callTimeoutMs = callTimeoutMs; }
    }

    public static class Heartbeat {
        private long intervalMs = 30000;
        private long timeoutMs = 120000;
        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
        public long getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
    }

    public static class Cleanup {
        private int resultRetentionDays = 30;
        public int getResultRetentionDays() { return resultRetentionDays; }
        public void setResultRetentionDays(int resultRetentionDays) { this.resultRetentionDays = resultRetentionDays; }
    }

    public static class Shutdown {
        private long awaitTerminationMs = 30000;
        public long getAwaitTerminationMs() { return awaitTerminationMs; }
        public void setAwaitTerminationMs(long awaitTerminationMs) { this.awaitTerminationMs = awaitTerminationMs; }
    }
}