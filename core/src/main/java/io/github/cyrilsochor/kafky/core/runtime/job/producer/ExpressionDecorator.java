package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertFalse;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNotNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

import com.ezylang.evalex.EvaluationException;
import com.ezylang.evalex.Expression;
import com.ezylang.evalex.config.ExpressionConfiguration;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.functions.FunctionIfc;
import com.ezylang.evalex.parser.ParseException;
import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordProducer;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.stream.Streams;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ExpressionDecorator extends AbstractRecordProducer {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionDecorator.class);

    public static RecordProducer of(final Map<Object, Object> cfg) throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        final Map<Object, Object> decorateHeaders = PropertiesUtils.getMap(cfg, KafkyProducerConfig.DECORATE_HEADERS);
        final Map<Object, Object> decorateValue = PropertiesUtils.getMap(cfg, KafkyProducerConfig.DECORATE_VALUE);
        if (MapUtils.isEmpty(decorateHeaders) && MapUtils.isEmpty(decorateValue)) {
            return null;
        } else {
            final List<Entry<String, FunctionIfc>> additionalFunctions = getCustomFunctions(
                    PropertiesUtils.getMap(cfg, KafkyProducerConfig.EXPRESSION_FUNCTIONS));
            final ExpressionConfiguration expressionConfiguration = ExpressionConfiguration.builder()
                    .dateTimeFormatters(List.of(DateTimeFormatter.ISO_INSTANT))
                    .build()
                    .withAdditionalFunctions(additionalFunctions.toArray(s -> new Entry[s]));
            final Map<String, Expression> headerExpressions = decorateHeaders.entrySet().stream()
                    .collect(toMap(
                            e -> e.getKey().toString(),
                            e -> createExpression(expressionConfiguration, e.getValue().toString())));
            final Map<List<String>, Expression> valueExpressions = decorateValue.entrySet().stream()
                    .collect(toMap(
                            e -> createPath(e.getKey().toString()),
                            e -> createExpression(expressionConfiguration, e.getValue().toString())));

            return new ExpressionDecorator(cfg, headerExpressions, valueExpressions);
        }
    }

    protected static Expression createExpression(final ExpressionConfiguration expressionConfiguration, final String expressionString) {
        return new Expression(expressionString, expressionConfiguration);
    }

    protected static List<String> createPath(final String pathString) {
        return Arrays.asList(pathString.split("\\."));
    }

    protected static ArrayList<Entry<String, FunctionIfc>> getCustomFunctions(Map<Object, Object> cfgFunctions)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
            SecurityException, ClassNotFoundException {
        final ArrayList<Entry<String, FunctionIfc>> functions = new ArrayList<>();
        for (Entry<Object, Object> f : cfgFunctions.entrySet()) {
            final FunctionIfc function = Class.forName(f.getValue().toString()).asSubclass(FunctionIfc.class).getConstructor().newInstance();
            functions.add(Map.entry(f.getKey().toString(), function));
        }
        return functions;
    }

    final Map<Object, Object> cfg;
    protected final Map<String, Expression> headerExpressions;
    protected final Map<List<String>, Expression> valueExpressions;

    public ExpressionDecorator(
            final Map<Object, Object> cfg,
            final Map<String, Expression> headerExpressions,
            final Map<List<String>, Expression> valueExpressions) {
        super();
        this.cfg = cfg;
        this.headerExpressions = headerExpressions;
        this.valueExpressions = valueExpressions;
    }

    @Override
    public int getPriority() {
        return 110;
    }

    @Override
    public ProducerRecord<Object, Object> produce() throws Exception {
        final ProducerRecord<Object, Object> source = getChainNext().produce();

        final Object value = source.value();
        assertNotNull(value, "Expected non-null value");
        assertTrue(value instanceof GenericRecord, "Expected value of class " + GenericRecord.class.getName());
        decorateValue((GenericRecord) value);

        final Headers headers = source.headers();
        assertNotNull(headers, "Expected non-null headers");
        decorateHeaders(headers);

        return source;
    }

    protected void decorateValue(final GenericRecord value) throws EvaluationException, ParseException {
        for (Entry<List<String>, Expression> decorateEntry : valueExpressions.entrySet()) {
            decorateValue(value, decorateEntry.getKey(), decorateEntry.getValue());
        }
    }

    protected void decorateValue(
            final GenericRecord value,
            final List<String> path,
            final Expression expression) throws EvaluationException, ParseException {
        assertFalse(path.isEmpty(), "Expected non-empty path");
        if (path.size() == 1) {
            final String field = path.get(0);
            final Object oldValue = value.get(field);
            final Object newValue = evaluateValueExpression(expression, oldValue);
            value.put(field, newValue);
        } else {
            final String field = path.get(0);
            final GenericRecord subValue = (GenericRecord) value.get(field);
            decorateValue(subValue, path.subList(1, path.size()), expression);
        }
    }

    protected void decorateHeaders(final Headers headers) throws EvaluationException, ParseException {
        for (Entry<String, Expression> decorateEntry : headerExpressions.entrySet()) {
            final String key = decorateEntry.getKey();
            final List<String> oldValues = Streams.of(headers.headers(key))
                    .map(v -> new String(v.value()))
                    .toList();
            headers.remove(key);
            final Expression expression = decorateEntry.getValue();
            LOG.atDebug().setMessage("Evaluating header {} expression {}, old values: {}")
                    .addArgument(key)
                    .addArgument(expression::getExpressionString)
                    .addArgument(oldValues)
                    .log();
            final List<String> newValues = evaluateHeaderExpression(expression, oldValues);
            for (final String newValue : newValues) {
                headers.add(key, newValue.getBytes());
            }
        }
    }

    protected List<String> evaluateHeaderExpression(final Expression expression, final List<String> oldValues)
            throws EvaluationException, ParseException {
        final EvaluationValue result = expression
                .with("oldValues", oldValues)
                .and("configuration", cfg)
                .evaluate();
        return toStringList(result);
    }

    protected Object evaluateValueExpression(final Expression expression, final Object oldValue) throws EvaluationException, ParseException {
        final EvaluationValue result = expression
                .with("oldValue", oldValue)
                .and("configuration", cfg)
                .evaluate();
        return result.getValue();
    }

    protected List<String> toStringList(final EvaluationValue result) {
        LOG.atDebug()
                .setMessage("Expression result type {} value: {}")
                .addArgument(() -> {
                    final Object value = result.getValue();
                    return value == null ? null : value.getClass().getName();
                })
                .addArgument(result::getValue)
                .log();

        if (result.isNullValue()) {
            return emptyList();
        } else if (result.isStringValue()) {
            return singletonList(result.getStringValue());
        } else if (result.isArrayValue()) {
            return result.getArrayValue().stream()
                    .flatMap(r -> toStringList(r).stream())
                    .toList();
        } else {
            return singletonList(result.getValue().toString());

        }
    }

    @Override
    public String getComponentInfo() {
        return super.getComponentInfo() +
                "("
                + "headers " + headerExpressions.keySet() + ", "
                + "value " + valueExpressions.keySet().stream()
                        .map(l -> l.stream().collect(Collectors.joining(".")))
                        .toList()
                +
                ")";
    }

}
