package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertFalse;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNotNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
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
import io.github.cyrilsochor.kafky.core.exception.ExpressionEvaluationException;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.stream.Streams;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
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

            final Map<Object, Object> constantArrayFiles = PropertiesUtils.getMap(cfg, KafkyProducerConfig.EXPRESSION_CONSTANT_ARRAY_FILES);
            final Map<String, Object> constants = constantArrayFiles == null ? emptyMap()
                    : constantArrayFiles
                    .entrySet().stream()
                    .collect(toMap(
                            e -> e.getKey().toString(),
                            e -> readCSV(e.getValue().toString())));

            return new ExpressionDecorator(cfg, headerExpressions, valueExpressions, constants);
        }
    }

    protected static List<Object> readCSV(final String path) {
        try {
            final List<Object> values = new ArrayList<>();

            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(Files.newBufferedReader(Path.of(path)));
            for (CSVRecord rec : records) {
                if (rec.size() == 0) {
                    values.add("");
                } else if (rec.size() == 1) {
                    values.add(rec.get(0));
                } else {
                    values.add(rec.toList());
                }
            }
            return values;
        } catch (Exception e) {
            throw new RuntimeException("Error parse CSV file " + path, e);
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

    protected final Map<Object, Object> cfg;
    protected final Map<String, Expression> headerExpressions;
    protected final Map<List<String>, Expression> valueExpressions;
    protected final Map<String, Object> constants;

    protected final AtomicLong sequenceNumber = new AtomicLong();

    public ExpressionDecorator(
            final Map<Object, Object> cfg,
            final Map<String, Expression> headerExpressions,
            final Map<List<String>, Expression> valueExpressions,
            final Map<String, Object> constants) {
        super();
        this.cfg = cfg;
        this.headerExpressions = headerExpressions;
        this.valueExpressions = valueExpressions;
        this.constants = constants;
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

        sequenceNumber.incrementAndGet();

        return source;
    }

    protected void decorateValue(final GenericRecord value) throws EvaluationException, ParseException {
        for (Entry<List<String>, Expression> decorateEntry : valueExpressions.entrySet()) {
            final List<String> valuePath = decorateEntry.getKey();
            final Expression expression = decorateEntry.getValue();
            try {
                decorateValue(value, valuePath, expression);
            } catch (Exception e) {
                throw new ExpressionEvaluationException(
                        valuePath.stream().collect(joining(".")),
                        expression,
                        e);
            }
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
        final EvaluationValue result = fillBaseValues(expression)
                .and("oldValues", oldValues)
                .evaluate();
        return toStringList(result);
    }

    protected Object evaluateValueExpression(final Expression expression, final Object oldValue) throws EvaluationException, ParseException {
        final EvaluationValue result = fillBaseValues(expression)
                .and("oldValue", oldValue)
                .evaluate();
        return result.getValue();
    }

    protected Expression fillBaseValues(final Expression expression) throws ParseException {
        final Expression exp = expression.copy();
        exp.and("configuration", cfg);
        exp.and("sequenceNumber", sequenceNumber.get());
        for (final Entry<String, Object> c : constants.entrySet()) {
            exp.and(c.getKey(), c.getValue());
        }
        return exp;
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
