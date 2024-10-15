package io.github.cyrilsochor.kafky.core.util;

public class InfoUtils {

    public static void appendFieldValue(final StringBuilder sb, final String fieldName, final Object value) {
        appendFieldKey(sb, fieldName);
        if (value == null) {
            sb.append("null");
        } else {
            sb.append(value.toString());
        }
    }

    public static void appendFieldKey(final StringBuilder sb, final String fieldName) {
        if (!sb.isEmpty()) {
            sb.append(", ");
        }

        sb.append(fieldName);
        sb.append(": ");
    }

    private InfoUtils() {
        // no instance
    }

}
