package io.github.cyrilsochor.kafky.core.util;

public class ObjectUtils {

    private ObjectUtils() {
        // no instance
    }

    public static long firstNonNullLong(final Number l0, final long l1) {
        if (l0 != null) {
            return l0.longValue();
        }

        return l1;
    }

}
