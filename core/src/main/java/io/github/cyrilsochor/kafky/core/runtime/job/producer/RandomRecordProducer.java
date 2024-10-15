package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static java.lang.Math.max;
import static java.lang.Math.min;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordProducer;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.exception.RandomMessageGeneratorException;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomRecordProducer extends AbstractRecordProducer {

    private static final Logger LOG = LoggerFactory.getLogger(RandomRecordProducer.class);

    protected final Class<? extends Object> valueClass;
    protected final Random random = new Random();

    public static RecordProducer of(final Map<Object, Object> cfg) throws ClassNotFoundException {
        final Class<? extends Object> valueClass = PropertiesUtils.getClass(cfg, Object.class,
                KafkyProducerConfig.GENERATOR_VALUE_CLASS);

        if (valueClass == null) {
            return null;
        }

        return new RandomRecordProducer(valueClass);
    }

    @Override
    public int getPriority() {
        return Integer.MIN_VALUE;
    }

    protected RandomRecordProducer(Class<? extends Object> valueClass) {
        super();
        this.valueClass = valueClass;
    }

    @Override
    public ProducerRecord<Object, Object> produce() {
        LOG.trace("Genering random message");

        final String key = random.nextBoolean() ? null : RandomStringUtils.insecure().next(8, false, true);
        final Object value = generateValue();
        final ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                "RND" + RandomStringUtils.insecure().next(8, true, false),
                key == null ? null : key.getBytes(),
                value);

        final int headersCount = RandomUtils.insecure().randomInt(0, 10);
        for (int i = 0; i < headersCount; i++) {
            producerRecord.headers().add("header_" + i, RandomStringUtils.insecure().next(15, true, true).getBytes());
        }

        LOG.debug("Generated record: {}", producerRecord);
        return producerRecord;
    }

    protected Object generateValue() {
        try {
            if (valueClass.isAssignableFrom(String.class)) {
                return RandomStringUtils.insecure().next(20, true, true).getBytes();
            } else {
                final Object value = valueClass.getConstructor().newInstance();
                if (value instanceof GenericRecord gr) {
                    generateGenericRecord(gr);
                }
                return value;
            }
        } catch (IllegalArgumentException
                | IllegalAccessException
                | InvocationTargetException
                | InstantiationException
                | NoSuchMethodException e) {
            throw new RandomMessageGeneratorException("Error generate message", e);
        }
    }

    protected void generateGenericRecord(GenericRecord gr) {
        for (final Field field : gr.getSchema().getFields()) {
            generateField(0, gr, field, field.schema());
        }
    }

    private void generateField(final int depth, final IndexedRecord rec, final Field field, final Schema schema) {
        final String name = field.name();
        final int index = field.pos();
        LOG.trace("Generating value of field name: {}, position: {}, field: {}", name, index, field);

        final Object value = generateFieldValue(depth, name, schema);
        rec.put(index, value);
    }

    private Object generateFieldValue(final int depth, final String fieldName, final Schema schema) {
        final Type valueType = schema.getType();
        return switch (valueType) {
        case STRING -> RandomStringUtils.insecure().next(20, true, true) + fieldName.substring(0, min(3, fieldName.length())).toUpperCase();
        case INT -> random.nextInt(10000);
        case LONG -> random.nextLong(1000000);
        case BOOLEAN -> random.nextBoolean();
        case FLOAT -> random.nextFloat();
        case DOUBLE -> random.nextDouble();
        case UNION -> generateFieldValue(depth, fieldName, schema.getTypes().get(random.nextInt(schema.getTypes().size())));
        case ENUM -> new GenericData.EnumSymbol(schema, schema.getEnumSymbols().get(random.nextInt(schema.getEnumSymbols().size())));
        case ARRAY -> {
            final List<Object> list = new LinkedList<>();
            final int length = random.nextInt(max(0, 6 - depth));
            for (int i = 0; i < length; i++) {
                list.add(generateFieldValue(depth, fieldName, schema.getElementType()));
            }
            yield list;
        }
        case RECORD -> {
            final Record rec = new Record(schema);
            for (final Field field : schema.getFields()) {
                generateField(depth + 1, rec, field, field.schema());
            }

            yield rec;
        }
        case BYTES -> {
            final byte[] bytes = new byte[random.nextInt(100)];
            random.nextBytes(bytes);
            yield bytes;
        }
        case NULL -> null;
        default -> {
            LOG.warn("Genarator doesn't support Filed type {} of field {}", valueType, fieldName);
            yield null;
        }
        };
    }

}
