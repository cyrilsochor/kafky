package io.github.cyrilsochor.kafky.core.util;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertNotNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import io.github.cyrilsochor.kafky.core.exception.InvalidPropertyType;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

public class PropertiesUtils {

    private PropertiesUtils() {
        // no instance
    }

    public static void addProperties(final Map<Object, Object> target, final Map<Object, Object> sourceLowerPriority) {
        addProperties(target, sourceLowerPriority, "");
    }

    public static void addProperties(final Map<Object, Object> target, final Map<Object, Object> sourceLowerPriority, final String prefix) {
        if (sourceLowerPriority != null) {
            sourceLowerPriority.forEach((k, v) -> {
                if (!target.containsKey(k)) {
                    target.put(prefix + k, v);
                }
            });
        }
    }

    private static <V> V get(final Map<Object, Object> properties, final String key, BiFunction<String, Object, V> parser) {
        final Object value = properties.get(key);
        return value == null ? null : parser.apply(key, value);
    }

    private static <V> V getRequired(final Map<Object, Object> properties, final String key, BiFunction<String, Object, V> parser) {
        return parser.apply(key, getRequired(properties, key));
    }

    public static Object getRequired(final Map<Object, Object> properties, final String key) {
        final Object value = properties.get(key);
        assertNotNull(value, () -> String.format("Property %s is mandatory", key));
        return value;
    }

    /* String */

    private static String parseString(final String key, final Object value) {
        if (value instanceof String string) {
            return string;
        } else {
            throw new InvalidPropertyType(key, value.getClass(), String.class);
        }
    }

    public static String getStringRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parseString);
    }

    public static String getString(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parseString);
    }

    /* Integer */

    private static int parseInteger(final String key, final Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        } else if (value instanceof String string) {
            return Integer.parseInt(string);
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Integer.class, String.class);
        }
    }

    public static int getIntegerRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parseInteger);
    }

    public static Integer getInteger(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parseInteger);
    }

    /* Long */

    private static long parseLong(final String key, final Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        } else if (value instanceof String string) {
            return Long.parseLong(string);
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Long.class, String.class);
        }
    }

    public static long getLongRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parseLong);
    }

    public static Long getLong(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parseLong);
    }

    /* Path */

    private static Path parsePath(final String key, final Object value) {
        if (value instanceof Path path) {
            return path;
        } else if (value instanceof String string) {
            return Path.of(string);
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Path.class, String.class);
        }
    }

    public static Path getPathRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parsePath);
    }

    public static Path getPath(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parsePath);
    }

    /* Duration */

    private static Duration parseDuration(final String key, final Object value) {
        if (value instanceof Duration duration) {
            return duration;
        } else if (value instanceof Number number) {
            return Duration.ofMillis(number.longValue());
        } else if (value instanceof String string) {
            return Duration.ofMillis(Long.parseLong(string));
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Duration.class, Number.class, String.class);
        }
    }

    public static Duration getDurationRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parseDuration);
    }

    public static Duration getDuration(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parseDuration);
    }

    /* List of Strings */

    private static List<String> parseListOfStrings(final String key, final Object value) {
        if (value instanceof List list) {
            return list;
        } else {
            throw new InvalidPropertyType(key, value.getClass(), List.class);
        }
    }

    public static List<String> getNonEmptyListOfStrings(final Map<Object, Object> properties, final String key) {
        final List<String> list = getRequired(properties, key, PropertiesUtils::parseListOfStrings);
        assertTrue(isNotEmpty(list), () -> format("At least one of %s is required", key));
        return list;
    }

    /* List of Maps */

    private static List<Map<Object, Object>> parseListOfMaps(final String key, final Object value) {
        if (value instanceof List list) {
            return list;
        } else {
            throw new InvalidPropertyType(key, value.getClass(), List.class);
        }
    }

    public static List<Map<Object, Object>> getNonEmptyListOfMaps(final Map<Object, Object> properties, final String key) {
        final List<Map<Object, Object>> list = getRequired(properties, key, PropertiesUtils::parseListOfMaps);
        assertTrue(isNotEmpty(list), () -> format("At least one of %s is required", key));
        return list;
    }

    /* Class */

    public static <T> Class<? extends T> parseClass(
            final String key,
            final Class<? extends T> clazz,
            final Object value)
            throws ClassNotFoundException {
        if (value instanceof Class valueClass) {
            return valueClass;
        } else if (value instanceof String string) {
            return Class.forName(string).asSubclass(clazz);
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Duration.class, Number.class, String.class);
        }
    }

    public static <T> Class<? extends T> getClassRequired(
            final Map<Object, Object> properties,
            final Class<? extends T> clazz,
            final String key) throws ClassNotFoundException {
        return parseClass(key, clazz, getRequired(properties, key));
    }

    public static <T> Class<? extends T> getClass(
            final Map<Object, Object> properties,
            final Class<? extends T> clazz,
            final String key) throws ClassNotFoundException {
        final Object value = properties.get(key);
        return value == null ? null : parseClass(key, clazz, value);
    }

    /* Object instance */

    private static <T> T createInstance(final Map<Object, Object> properties, final Class<? extends T> valueClass)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        final Constructor<? extends T> constructor = valueClass.getConstructor(Properties.class);
        return constructor.newInstance(properties);
    }

    public static <T> T getObjectInstance(
            final Map<Object, Object> properties,
            final Class<? extends T> clazz,
            final String key) throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        final Class<? extends T> value = getClass(properties, clazz, key);
        return value == null ? null : createInstance(properties, value);
    }

    public static <T> T getObjectInstanceRequired(
            final Map<Object, Object> properties,
            final Class<? extends T> clazz,
            final String key) throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        return createInstance(properties, getClassRequired(properties, clazz, key));
    }

}
