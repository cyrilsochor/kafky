package io.github.cyrilsochor.kafky.core.util;

public class ObjectUtils {

    public static long firstNonNullLong(final Number l0, final long l1) {
        if (l0 != null) {
            return l0.longValue();
        }

        return l1;
    }

    private ObjectUtils() {
        // no instance
    }

}
