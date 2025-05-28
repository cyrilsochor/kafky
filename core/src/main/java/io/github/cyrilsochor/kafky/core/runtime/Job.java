package io.github.cyrilsochor.kafky.core.runtime;

import io.github.cyrilsochor.kafky.api.job.JobState;

public interface Job {

    String getId();

    String getName();

    default String getInfo() {
        return "";
    }

    JobState getState();

    void setState(final JobState jobState);

    default IterationResult prepare() throws Exception {
        return IterationResult.stop();
    }

    default IterationResult start() throws Exception {
        return IterationResult.stop();
    }

    default boolean isObserve() {
        return false;
    }

    default IterationResult warmUp() throws Exception {
        return IterationResult.stop();
    }

    default IterationResult measureResponseTime() throws Exception {
        return IterationResult.stop();
    }

    default IterationResult measureThroughput() throws Exception {
        return IterationResult.stop();
    }

    default IterationResult observe() throws Exception {
        return IterationResult.stop();
    }

    default void finish() throws Exception {
    }

    default void shutdownHook() {
    }

    default long getAsyncTasksCount() {
        return 0;
    }

}
