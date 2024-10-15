package io.github.cyrilsochor.kafky.core.storage.text;

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
import java.util.Collection;

public class IdexedRecordSerializer extends JsonSerializer<IndexedRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(IdexedRecordSerializer.class);

    @Override
    public void serialize(
            final IndexedRecord value,
            final JsonGenerator gen,
            final SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeObjectFieldStart(AVROConstants.RECORD);

        gen.writeStringField(AVROConstants.CLASS, value.getSchema().getNamespace() + "." + value.getSchema().getName());
        gen.writeObjectFieldStart(AVROConstants.DATA);

        writeData(value, gen);

        gen.writeEndObject();

        gen.writeStringField(AVROConstants.SCHEMA, value.getSchema().toString(false));

        gen.writeEndObject();
        gen.writeEndObject();
    }

    protected void writeData(final IndexedRecord record, final JsonGenerator gen) throws IOException {
        final Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            writeField(field, record.get(field.pos()), gen);
        }
    }

    private void writeField(final Field field, final Object value, final JsonGenerator gen) throws IOException {
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
        case BYTES -> gen.writeBinaryField(name, (byte[]) value);
        case ENUM -> gen.writeStringField(name, ((GenericData.EnumSymbol) value).toString());
        case UNION -> {
            if (value != null) {
                gen.writePOJOField(name, value);
            }
        }
        case ARRAY -> {
            gen.writeArrayFieldStart(name);

            final Schema elementSchema = schema.getElementType();
            final Type elementType = elementSchema.getType();
            final Collection<Object> collection = (Collection<Object>) value;
            for (Object element : collection) {
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

            gen.writeEndArray();
        }
        default -> 
                LOG.warn("Nonimplemented type {} of field {}", type, name);
        }
    }

}
