package io.github.cyrilsochor.kafky.api.component;

public interface Component {

    default void init() throws Exception {
    }

    default void close() throws Exception {
    }

    default String getComponentInfo() {
        return this.getClass().getSimpleName();
    }

}