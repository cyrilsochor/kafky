package io.github.cyrilsochor.kafky.core.config;

import io.github.cyrilsochor.kafky.core.runtime.job.producer.RecordProducer;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkyDefaults {

    public static final Map<Object, Object> DEFAULT_CONSUMER_PROPERTIES = Map.of(
            "key.deserializer","org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer","io.apicurio.registry.serde.avro.AvroKafkaDeserializer",
            //            "value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "group.id", "kafky" + RandomStringUtils.insecure().next(20, true, true)
            );

            public static final Map<Object, Object> DEFAULT_PRODUCER_PROPERTIES = Map.of(
            "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer",
            "value.serializer", "io.apicurio.registry.serde.avro.AvroKafkaSerializer",
            // "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer",
            KafkyProducerConfig.RECORD_PRODUCERS_PACKAGE, List.of(RecordProducer.class.getPackage().getName())
    );

    public static final Set<String> TRANSPORT_HEADERS = Set.of(
            "apicurio.value.globalId",
            "apicurio.value.encoding");

    public static long DEFAULT_REPORT_INTERVAL = 10_000;

    private KafkyDefaults() {
        // no instance
    }

}