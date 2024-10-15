package io.github.cyrilsochor.kafky.core.util;

public interface Consumer<V> {

    default void init() throws Exception {
    }

    void consume(V value) throws Exception;

    default void close() throws Exception {
    }

}
