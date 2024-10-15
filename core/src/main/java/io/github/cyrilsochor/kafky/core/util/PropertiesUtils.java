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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

@SuppressWarnings({
        "unchecked",
        "java:S3740",
})
public class PropertiesUtils {

    protected static final Pattern PATH_SUFFIX_PATTERN = Pattern.compile("(\\.[^.]*$)");

    private PropertiesUtils() {
        // no instance
    }

    public static void addProperties(final Map<Object, Object> target, final Map<Object, Object> sourceLowerPriority) {
        addProperties(target, sourceLowerPriority, "");
    }

    public static void addProperties(final Map<Object, Object> target, final Map<Object, Object> sourceLowerPriority, final String prefix) {
        if (sourceLowerPriority != null) {
            sourceLowerPriority.forEach((key, sValue) -> {
                if (!target.containsKey(key)) {
                    target.put(prefix + key, sValue);
                } else if (sValue instanceof Map sValueMap) {
                    final Object tValue = target.get(key);
                    assertTrue(tValue instanceof Map,
                            () -> format("Expected %s, actual %s", Map.class.getName(), tValue.getClass().getName()));
                    addProperties((Map<Object, Object>) tValue, sValueMap);
                }
            });
        }
    }

    private static <V> V get(final Map<Object, Object> properties, final String key, BiFunction<String, Object, V> parser) {
        return getOrElse(properties, key, parser, null);
    }

    private static <V> V getOrElse(Map<Object, Object> properties, String key, BiFunction<String, Object, V> parser, V elseValue) {
        final Object value = properties.get(key);
        if (value == null) {
            return elseValue;
        } else {
            return parser.apply(key, value);
        }
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

    /* Booelean */

    private static boolean parseBoolean(final String key, final Object value) {
        if (value instanceof Boolean b) {
            return b;
        } else if (value instanceof String s) {
            return Boolean.parseBoolean(s);
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Long.class, String.class);
        }
    }

    public static boolean getBooleanRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parseBoolean);
    }

    public static Boolean getBoolean(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parseBoolean);
    }

    /* Path */

    private static Path parsePath(final String key, final Object value, final String suffix) {
        if (value instanceof Path path) {
            return path;
        } else if (value instanceof String string) {
            final String fp = suffix == null ? string : appendPathSuffix(string, suffix);
            return Path.of(fp);
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Path.class, String.class);
        }
    }

    protected static String appendPathSuffix(final String path, final String suffix) {
        return PATH_SUFFIX_PATTERN.matcher(path).replaceAll(suffix + "$1");
    }

    public static Path getPathRequired(final Map<Object, Object> properties, final String key) {
        return getPathRequired(properties, key, null);
    }

    public static Path getPathRequired(final Map<Object, Object> properties, final String key, final String suffixKey) {
        return getRequired(properties, key, (t, u) -> parsePath(t, u, getString(properties, suffixKey)));
    }

    public static Path getPath(final Map<Object, Object> properties, final String key) {
        return getPath(properties, key, null);
    }

    public static Path getPath(final Map<Object, Object> properties, final String key, final String suffixKey) {
        return get(properties, key, (t, u) -> parsePath(t, u, getString(properties, suffixKey)));
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

    public static List<String> getListOfStrings(final Map<Object, Object> properties, final String key) {
        return getOrElse(properties, key, PropertiesUtils::parseListOfStrings, Collections.emptyList());
    }

    public static List<String> getNonEmptyListOfStrings(final Map<Object, Object> properties, final String key) {
        final List<String> list = getRequired(properties, key, PropertiesUtils::parseListOfStrings);
        assertTrue(isNotEmpty(list), () -> format("At least one of %s is required", key));
        return list;
    }

    /* Map */

    private static Map<Object, Object> parseMap(final String key, final Object value) {
        if (value instanceof Map map) {
            return map;
        } else {
            throw new InvalidPropertyType(key, value.getClass(), Map.class);
        }
    }

    public static Map<Object, Object> getMapRequired(final Map<Object, Object> properties, final String key) {
        return getRequired(properties, key, PropertiesUtils::parseMap);
    }

    public static Map<Object, Object> getMapOrInsert(final Map<Object, Object> properties, final String key) {
        return (Map<Object, Object>) properties.computeIfAbsent(key, k -> new HashMap<>());
    }

    public static Map<Object, Object> getMap(final Map<Object, Object> properties, final String key) {
        return get(properties, key, PropertiesUtils::parseMap);
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
        final Constructor<? extends T> constructor = valueClass.getConstructor(Map.class);
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
