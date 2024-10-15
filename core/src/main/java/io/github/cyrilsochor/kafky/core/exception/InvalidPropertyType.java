package io.github.cyrilsochor.kafky.core.exception;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

import java.util.List;
import java.util.stream.Collectors;

public class InvalidPropertyType extends RuntimeException {

    private final String key;

    private final Class<?> actualClass;

    private final List<Class<?>> expectedClasses;

    public InvalidPropertyType(
            final String key,
            final Class<?> actualClass,
            final Class<?>... expectedClasses) {
        super(format("Unexpected value class %s for property %s, expected one of %s",
                actualClass,
                key,
                stream(expectedClasses)
                        .map(Class::getName)
                        .collect(Collectors.joining(", "))));
        this.key = key;
        this.actualClass = actualClass;
        this.expectedClasses = asList(expectedClasses);
    }

    public Class<?> getActualClass() {
        return actualClass;
    }

    public List<Class<?>> getExpectedClasses() {
        return expectedClasses;
    }

    public String getKey() {
        return key;
    }

}
