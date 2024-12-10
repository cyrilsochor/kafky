package io.github.cyrilsochor.kafky.core.config;

public class KafkyExecutorConfig {

    public static final String CORE_POOL_SIZE = "core-pool-size";
    public static final String MAXIMUM_POOL_SIZE = "maximum-pool-size";
    public static final String KEEP_ALIVE_TIME = "keep-alive-time";
    public static final String WORK_QUEUE_SIZE = "work-queue-size";
    public static final String ALLOW_CORE_THREAD_TIME_OUT = "allow-core-thread-time-out";

    private KafkyExecutorConfig() {
        // no instance
    }

}
