package io.github.cyrilsochor.kafky.core.util;

public interface Producer<V> {

    default void init() throws Exception {
    }

    V produce() throws Exception;

    default void close() throws Exception {
    }

    default int getPriority() {
        return 0;
    }

}
