package io.github.cyrilsochor.kafky.core.util;

import io.github.cyrilsochor.kafky.core.exception.AssertException;

import java.util.function.Supplier;

public class Assert {

    public static void assertTrue(final boolean condition, final String errorMessage) {
        if (!condition) {
            throw new AssertException(errorMessage);
        }
    }

    public static void assertTrue(final boolean condition, final Supplier<String> errorMessageProvider) {
        if (!condition) {
            throw new AssertException(errorMessageProvider.get());
        }
    }

    public static void assertFalse(final boolean condition, final String errorMessage) {
        assertTrue(!condition, errorMessage);
    }

    public static void assertFalse(final boolean condition, final Supplier<String> errorMessageProvider) {
        assertTrue(!condition, errorMessageProvider);
    }

    public static void assertNotNull(final Object object, final String errorMessage) {
        assertTrue(object != null, errorMessage);
    }

    public static void assertNotNull(final Object object, final Supplier<String> errorMessageProvider) {
        assertTrue(object != null, errorMessageProvider);
    }

    public static void assertNull(final Object object, final String errorMessage) {
        assertTrue(object == null, errorMessage);
    }

    public static void assertNull(final Object object, final Supplier<String> errorMessageProvider) {
        assertTrue(object == null, errorMessageProvider);
    }

}
