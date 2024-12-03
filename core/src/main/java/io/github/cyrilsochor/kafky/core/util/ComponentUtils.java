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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
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
        return findImplementationsChain(subject, subjectClass, packages, List.of(new ImplementationParameter(Map.class, cfg)));
    }

    public static <S extends ChainComponent<S>> S findImplementationsChain(
            final String subject,
            final Class<? extends S> subjectClass,
            final List<String> packages,
            List<ImplementationParameter> parameters) {
        final LinkedList<S> impls = findImplementations(
                subject,
                subjectClass,
                packages,
                parameters,
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
        return findImplementations(subject, subjectClass, packages, List.of(new ImplementationParameter(Map.class, cfg)), orderComparator);
    }

    public static class ImplementationParameter {
        private final Class<?> valueClass;
        private final Object value;

        public ImplementationParameter(Class<?> valueClass, Object value) {
            super();
            this.valueClass = valueClass;
            this.value = value;
        }

        public Class<?> getValueClass() {
            return valueClass;
        }

        public Object getValue() {
            return value;
        }

    }

    public static <S> LinkedList<S> findImplementations(
            final String subject,
            final Class<? extends S> subjectClass,
            final List<String> packages,
            final List<ImplementationParameter> parameters,
            final Comparator<S> orderComparator) {
        final Stream<S> nonsorted = packages.stream()
                .peek(producersPackage -> LOG.debug("Finding {}s in packages {}", subject, producersPackage))
                .flatMap(implPackage -> new Reflections(implPackage, new SubTypesScanner(false)).getSubTypesOf(subjectClass).stream())
                .peek(producerClass -> LOG.debug("Found {} class {}", subject, producerClass.getName()))
                .filter(implClass -> !Modifier.isAbstract(implClass.getModifiers()))
                .map(implClass -> (S) createImplementation(subject, implClass, parameters))
                .filter(Objects::nonNull);
        final Stream<S> stream = orderComparator == null ? nonsorted
                : nonsorted.sorted(orderComparator);
        return stream
                .peek(p -> LOG.debug("Found {}: {}", subject, p))
                .collect(toCollection(LinkedList::new));
    }

    @SuppressWarnings("unchecked")
    // nullable
    public static <S> S createImplementation(
            final String subject,
            final Class<? extends S> implementationClass,
            final List<ImplementationParameter> parameters) {
        try {
            m: for (final Method method : implementationClass.getMethods()) {
                if(!"of".equals(method.getName())){
                    continue;
                }
                LOG.trace("Found factory method {}", method);
                final List<Object> factoryMethodParameterValues = new ArrayList<>();
                dp: for (final Parameter declaredParameter : method.getParameters()) {
                    for (ImplementationParameter suppliedParameter : parameters) {
                        if (declaredParameter.getType().isAssignableFrom(suppliedParameter.getValueClass())) {
                            factoryMethodParameterValues.add(suppliedParameter.getValue());
                            continue dp;
                        }
                    }
                    LOG.trace("Factory method {} is not invocable, unknown parameter {}", method, declaredParameter);
                    continue m;
                }
                final Object impl = method.invoke(null, factoryMethodParameterValues.toArray());
                if (impl == null) {
                    LOG.debug("Skipping {} {} - factory method returned null", subject, implementationClass.getName());
                    return null;
                } else if (implementationClass.isAssignableFrom(impl.getClass())) {
                    return (S) impl;
                }
                throw new InvalidConfigurationException(format("Exxpected return type %s of factory method %s return type",
                        implementationClass.getName(),
                        method));
            }
            throw new InvalidConfigurationException(format("Factory method not found for %s class %s", subject, implementationClass.getName()));
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
            throw new InvalidConfigurationException(format("Invalid factory method of %s class %s", subject, implementationClass.getName()), e);
        }
    }

    private ComponentUtils() {
        // no instance
    }

}
