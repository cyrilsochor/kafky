package io.github.cyrilsochor.kafky.core.serde;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.github.cyrilsochor.kafky.core.storage.text.IdexedRecordSerializer;
import org.apache.avro.generic.IndexedRecord;

public class Serdes {

    static final ObjectMapper OBJECT_MAPPER;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(IndexedRecord.class, new IdexedRecordSerializer());
        OBJECT_MAPPER = YAMLMapper.builder()
                .findAndAddModules()
                .serializationInclusion(NON_NULL)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, true)
                .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
                .configure(YAMLGenerator.Feature.ALLOW_LONG_KEYS, true)
                .addModule(module)
                .build();
    }

    public static ObjectMapper getDefaultObjectMapper() {
        return OBJECT_MAPPER;
    }

    private Serdes() {
        // no instance
    }

}
