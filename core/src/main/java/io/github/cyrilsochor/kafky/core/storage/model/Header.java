package io.github.cyrilsochor.kafky.core.storage.model;

public record Header(
        String key,
        Object value) {
    String getX() {
        return "bus";
    }
}
