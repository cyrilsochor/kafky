package io.github.cyrilsochor.kafky.core.storage.model;

import java.time.LocalDateTime;
import java.util.Collection;

public record Message(
        String topic,
        Integer partition,
        Long offset,
        LocalDateTime timestamp,
        Collection<Header> headers,
        Object key,
        Object value
) {
}
