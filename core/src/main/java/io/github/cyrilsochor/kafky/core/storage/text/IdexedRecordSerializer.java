package io.github.cyrilsochor.kafky.core.storage.text;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("unchecked")
public class IdexedRecordSerializer extends JsonSerializer<IndexedRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(IdexedRecordSerializer.class);

    @Override
    public void serialize(
            final IndexedRecord value,
            final JsonGenerator gen,
            final SerializerProvider serializers) throws IOException {
        LOG.trace("Start serialize {}", value);

        gen.writeStartObject();
        gen.writeObjectFieldStart(AVROConstants.RECORD);

        gen.writeStringField(AVROConstants.CLASS, value.getSchema().getNamespace() + "." + value.getSchema().getName());
        gen.writeObjectFieldStart(AVROConstants.DATA);

        writeData(value, gen);

        gen.writeEndObject();

        gen.writeStringField(AVROConstants.SCHEMA, value.getSchema().toString(false));

        gen.writeEndObject();
        gen.writeEndObject();

        LOG.trace("Finish serialize {}", value);
    }

    protected void writeData(final IndexedRecord rec, final JsonGenerator gen) throws IOException {
        final Schema schema = rec.getSchema();
        for (Field field : schema.getFields()) {
            writeField(field, rec.get(field.pos()), gen);
        }
    }

    protected void writeField(final Field field, final Object value, final JsonGenerator gen) throws IOException {
        final String name = field.name();
        final Schema schema = field.schema();
        final Type type = schema.getType();
        switch (type) {
        case STRING -> gen.writeStringField(name, (String) value);
        case INT -> gen.writeNumberField(name, ((Number) value).intValue());
        case LONG -> gen.writeNumberField(name, ((Number) value).longValue());
        case FLOAT -> gen.writeNumberField(name, ((Number) value).floatValue());
        case DOUBLE -> gen.writeNumberField(name, ((Number) value).doubleValue());
        case BOOLEAN -> gen.writeBooleanField(name, (Boolean) value);
        case BYTES -> {
            if (value instanceof byte[] ba) {
                gen.writeBinaryField(name, ba);
            } else if (value instanceof ByteBuffer bb) {
                gen.writeBinaryField(name, bb.array());
            } else {
                LOG.warn("Nonimplemented type {} with value class {} of field {}", type, value.getClass().getName(), name);
            }
        }
        case ENUM -> gen.writeStringField(name, ((GenericData.EnumSymbol) value).toString());
        case UNION -> {
            if (value == null) {
                // do nothing
            } else if (value instanceof IndexedRecord ir) {
                gen.writeObjectFieldStart(name);
                writeData(ir, gen);
                gen.writeEndObject();
            } else if (value instanceof GenericData.Array array) {
                final List<Schema> nonNullSchemas = schema.getTypes().stream()
                        .filter(s -> s.getType() != Type.NULL)
                        .toList();
                assertTrue(nonNullSchemas.size() == 1, "Expected exactly one non-null schema of filed " + name);
                final Schema unionSchema = nonNullSchemas.get(0);
                final Schema elementSchema = unionSchema.getElementType();
                final Type elementType = elementSchema.getType();
                writeArray(gen, name, type, elementType, array);
            } else {
                gen.writePOJOField(name, value);
            }
        }
        case RECORD -> {
            gen.writeObjectFieldStart(name);
            writeData((IndexedRecord) value, gen);
            gen.writeEndObject();
        }
        case ARRAY -> {
            final Schema elementSchema = schema.getElementType();
            final Type elementType = elementSchema.getType();
            writeArray(gen, name, type, elementType, (Collection<Object>) value);
        }
        default -> LOG.warn("Nonimplemented type {} with value class {} of field {}", type, value.getClass().getName(), name);
        }
    }

    protected void writeArray(final JsonGenerator gen, final String name, final Type type, final Type elementType,
            final Collection<Object> collection) throws IOException {
        gen.writeArrayFieldStart(name);
        for (Object element : collection) {
            writeArrayElement(gen, name, type, elementType, element);
        }
        gen.writeEndArray();
    }

    protected void writeArrayElement(final JsonGenerator gen, final String name, final Type type, final Type elementType, Object element)
            throws IOException {
        switch (elementType) {
        case STRING -> gen.writeString((String) element);
        case INT -> gen.writeNumber(((Number) element).intValue());
        case LONG -> gen.writeNumber(((Number) element).longValue());
        case FLOAT -> gen.writeNumber(((Number) element).floatValue());
        case DOUBLE -> gen.writeNumber(((Number) element).doubleValue());
        case RECORD -> {
            gen.writeStartObject();
            writeData((IndexedRecord) element, gen);
            gen.writeEndObject();
        }
        default -> LOG.warn("Nonimplemented {} element type {} of field {}", type, elementType, name);
        }
    }

}
