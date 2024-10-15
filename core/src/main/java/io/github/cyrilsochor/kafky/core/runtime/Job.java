package io.github.cyrilsochor.kafky.core.runtime;

public interface Job {

    String getId();

    String getName();

    default IterationResult prepare() throws Exception {
        return IterationResult.stop();
    }

    default IterationResult start() throws Exception {
        return IterationResult.stop();
    }

    IterationResult run() throws Exception;

    default void finish() throws Exception {
    }

    default String getInfo() {
        return "";
    };

}
