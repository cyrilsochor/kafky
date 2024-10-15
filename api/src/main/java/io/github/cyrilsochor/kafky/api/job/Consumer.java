package io.github.cyrilsochor.kafky.api.job;

public interface Consumer<V> {

    default void init() throws Exception {
    }

    void consume(V value) throws Exception;

    default void close() throws Exception {
    }

}
