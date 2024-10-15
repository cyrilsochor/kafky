package io.github.cyrilsochor.kafky.core.storage.mapper;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static java.lang.String.format;

import io.github.cyrilsochor.kafky.api.job.Producer;
import io.github.cyrilsochor.kafky.core.exception.InvalidSchemaException;
import io.github.cyrilsochor.kafky.core.storage.model.Header;
import io.github.cyrilsochor.kafky.core.storage.model.Message;
import io.github.cyrilsochor.kafky.core.storage.text.AVROConstants;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.print.DocFlavor.BYTE_ARRAY;

public class StorageDeserializer implements Producer<ConsumerRecord<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(StorageDeserializer.class);

    protected final Producer<Message> producer;

    public StorageDeserializer(Producer<Message> producer) {
        this.producer = producer;
    }

    @Override
    public void init() throws Exception {
        producer.init();
    }

    @Override
    public ConsumerRecord<?, ?> produce() throws Exception {
        final Message message = producer.produce();

        if (message == null) {
            return null;
        }

        LOG.debug("Converting {} to {}: {}", message.getClass().getName(), ConsumerRecord.class.getName(), message);

        final Object orgKey = message.key();
        final Object newKey;
        if (orgKey == null) {
            newKey = null;
        } else if (orgKey instanceof byte[] b) {
            newKey = b;
        } else {
            newKey = orgKey.toString().getBytes();
        }

        final Object orgValue = message.value();
        Object newValue = orgValue;
        if (orgValue != null) {
            if (orgValue instanceof Map<?, ?> map) {
                if (map.containsKey(AVROConstants.RECORD)) {
                    newValue = toIndexedRecord((Map<?, ?>) map.get(AVROConstants.RECORD));
                }
            }
        }

        final ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                message.topic(),
                message.partition(),
                message.offset(),
                newKey,
                newValue);

        for (final Header headerTemplate : message.headers()) {
            final String headerKey = headerTemplate.key();
            final Object headerValueTemplate = headerTemplate.value();

            final byte[] headerBytes;
            if (headerValueTemplate == null) {
                headerBytes = null;
            } else if (headerValueTemplate instanceof byte[] ba) {
                headerBytes = ba;
            } else if (headerValueTemplate instanceof String s) {
                headerBytes = s.getBytes();
            } else {
                throw new IllegalStateException("Unsupported header value class " + headerValueTemplate.getClass().getName());
            }

            record.headers().add(headerKey, headerBytes);
        }

        LOG.debug("Converted: {}", message);
        return record;
    }

    protected Object toIndexedRecord(final Map<?, ?> map) {
        LOG.debug("Transforming value {} to IndexedRecord", map);
        final String schemaJson = (String) map.get(AVROConstants.SCHEMA);
        try {
            final Schema schema = new Schema.Parser().parse(schemaJson);
            return valueFromTemplate(map.get(AVROConstants.DATA), schema);
        } catch (Exception e) {
            throw new InvalidSchemaException(schemaJson, e);
        }
    }

    protected Object valueFromTemplate(final Object source, final Schema schema) {
        final String name = schema.getName();
        final Type type = schema.getType();
        LOG.debug("Fill name {}, type {}: {}", name, type, source);

        return switch (type) {
        case INT -> ((Number) source).intValue();
        case LONG -> ((Number) source).longValue();
        case FLOAT -> ((Number) source).floatValue();
        case DOUBLE -> ((Number) source).doubleValue();
        case STRING -> source;
        case BOOLEAN -> source;
        case BYTES -> source;
        case ENUM -> new GenericData.EnumSymbol(schema, source);
        case UNION -> {
            LOG.trace("{} types: {}", type, schema.getTypes());
            final List<Schema> possibleSchemas = new LinkedList<>();
            for (Schema unionSchema : schema.getTypes()) {
                if (unionSchema.getType() == Type.NULL) {
                    if (source == null) {
                        yield null;
                    }
                } else {
                    possibleSchemas.add(unionSchema);
                }
            }
            assertTrue(possibleSchemas.size() == 1, () -> format("Expected one %s subschema, possible schemas: %s", type, possibleSchemas));
            yield valueFromTemplate(source, possibleSchemas.get(0));
        }
        case ARRAY -> {
            final List<Object> list = new LinkedList<>();
            for (Object sourceElement : (Iterable<?>) source) {
                list.add(valueFromTemplate(sourceElement, schema.getElementType()));
            }
            yield list;
        }
        case RECORD -> {
            final Record record = new GenericData.Record(schema);
            final Map<?, ?> sourceMap = (Map<?, ?>) source;
            for (Field field : schema.getFields()) {
                record.put(field.pos(), valueFromTemplate(sourceMap.get(field.name()), field.schema()));
            }
            yield record;
        }
        default -> {
            LOG.warn("Unknown type {} of template field {}", type, name);
            yield null;
        }
        };
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

}
