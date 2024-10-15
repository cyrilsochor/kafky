package io.github.cyrilsochor.kafky.core.util;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertFalse;
import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;

import io.github.cyrilsochor.kafky.api.component.ChainComponent;
import io.github.cyrilsochor.kafky.core.exception.InvalidConfigurationException;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class ComponentUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ComponentUtils.class);

    public static <S> LinkedList<S> findImplementations(
            final String subject,
            final Class<? extends S> subjectClass,
            final List<String> packages,
            final Map<Object, Object> cfg) {
        return findImplementations(subject, subjectClass, packages, cfg, null);
    }

    public static <S extends ChainComponent<S>> S findImplementationsChain(
            final String subject,
            final Class<? extends S> subjectClass,
            final List<String> packages,
            final Map<Object, Object> cfg) {
        final LinkedList<S> impls = findImplementations(
                subject,
                subjectClass,
                packages,
                cfg,
                Comparator.<S>comparingInt(p -> p.getPriority()).reversed()
                        .thenComparing(p -> p.getClass().getSimpleName()));

        assertFalse(impls.isEmpty(), () -> format("No %s found in packages %s", subject, packages));

        S nextProducer = null;
        for (Iterator<S> it = impls.descendingIterator(); it.hasNext();) {
            final S impl = it.next();
            impl.setChainNext(nextProducer);
            nextProducer = impl;
        }

        return impls.getFirst();
    }

    public static <S> LinkedList<S> findImplementations(
            final String subject,
            final Class<? extends S> subjectClass,
            final List<String> packages,
            final Map<Object, Object> cfg,
            final Comparator<S> orderComparator) {
        final Stream<S> nonsorted = packages.stream()
                .peek(producersPackage -> LOG.debug("Finding {}s in packages {}", subject, producersPackage))
                .flatMap(producersPackage -> new Reflections(producersPackage, new SubTypesScanner(false)).getSubTypesOf(subjectClass).stream())
                .peek(producerClass -> LOG.debug("Found {} class {}", subject, producerClass.getName()))
                .filter(producerClass -> !Modifier.isAbstract(producerClass.getModifiers()))
                .map(producerClass -> {
                    try {
                        final S producer = (S) producerClass.getMethod("of", Map.class).invoke(null, cfg);
                        if (producer == null) {
                            LOG.debug("Skipping {} {} - factory method returned null", subject, producerClass.getName());
                        }
                        return producer;
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                            | SecurityException e) {
                        throw new InvalidConfigurationException(format("Invalid factory metod of %s class %s", subject, producerClass.getName()), e);
                    }
                })
                .filter(Objects::nonNull);
        final Stream<S> stream = orderComparator == null ? nonsorted
                : nonsorted.sorted(orderComparator);
        return stream
                .peek(p -> LOG.debug("Found {}: {}", subject, p))
                .collect(toCollection(LinkedList::new));
    }

    private ComponentUtils() {
        // no instance
    }

}
