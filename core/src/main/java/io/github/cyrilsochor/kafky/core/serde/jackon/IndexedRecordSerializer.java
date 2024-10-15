package io.github.cyrilsochor.kafky.core.serde.jackon;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class IndexedRecordSerializer extends StdSerializer<IndexedRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexedRecordSerializer.class);

    protected static record ValueWithSchema(
            String value,
            Schema schema) {
    }

    public IndexedRecordSerializer() {
        this(null);
    }

    public IndexedRecordSerializer(Class<IndexedRecord> t) {
        super(t);
    }

    @Override
    public void serialize(
            IndexedRecord value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {

        jgen.writeStartObject();
        jgen.writeStringField("itemName", "xxx");
        jgen.writeEndObject();
    }

    //    @Nullable
    //    protected ValueWithSchema bytes2YamlAndSchema(@Nullable byte[] bytes) {
    //        if (bytes == null) {
    //            return null;
    //        } else {
    //            final GenericRecord deserializedValue = (GenericRecord) this.deserialize(bytes); // asi z AbstractKafkaAvroDeserializer
    //            return genericRecord2YamlWithSchema(deserializedValue);
    //        }
    //    }

    protected Map<String, Object> genericRecord2Map(final IndexedRecord record) {
        final Map<String, Object> ret = new HashMap<String, Object>();

        for (Field field : record.getSchema().getFields()) {
            final String fieldName = field.name();
            final Object value = genericRecordFieldValue(record, field);
            ret.put(fieldName, value);
        }

        return ret;
    }

    protected Object genericRecordFieldValue(IndexedRecord record, Field field) {
        LOG.info("genericRecordFieldValue {}, {} - {}", field.name(), field.schema().getType(), field.schema().getClass());
        final Object value = record.get(field.pos());
        switch (field.schema().getType()) {
        case ENUM:
            return value.toString();

        case UNION:
            LOG.info("genericRecordFieldValue UNION {}: {}", value == null ? "" : value.getClass(), value);
            for (Schema s : field.schema().getTypes()) {
                LOG.info("genericRecordFieldValue UNION subschema {}, {}", s.getFullName(), s.getLogicalType());
            }
            return value;
        case RECORD:
            return genericRecord2Map((IndexedRecord) value);
        default:
            return value;
        }
    }

}