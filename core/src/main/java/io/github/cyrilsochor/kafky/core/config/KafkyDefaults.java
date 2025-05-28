package io.github.cyrilsochor.kafky.core.config;

import io.github.cyrilsochor.kafky.core.expression.funtion.NewB3Function;
import io.github.cyrilsochor.kafky.core.expression.funtion.RandomLongFunction;
import io.github.cyrilsochor.kafky.core.expression.funtion.RandomUUIDFunction;
import io.github.cyrilsochor.kafky.core.expression.funtion.SizeFunction;
import io.github.cyrilsochor.kafky.core.runtime.job.consumer.StorageRecordConsumer;
import io.github.cyrilsochor.kafky.core.runtime.job.producer.TemplateRecordProducer;

import java.util.List;
import java.util.Map;

public class KafkyDefaults {

    public static final Map<Object, Object> DEFAULT_CONSUMER_CONFIGURATION = Map.of(
            KafkyConsumerConfig.RECORD_CONSUMERS_PACKAGES, List.of(StorageRecordConsumer.class.getPackage().getName()),
            KafkyConsumerConfig.PROPERITES, Map.of(
                    "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
            KafkyConsumerConfig.OBSERVE, true);

    public static final Map<Object, Object> DEFAULT_PRODUCER_CONFIGURATION = Map.of(
            KafkyProducerConfig.MESSAGES_COUNT, 1l,
            KafkyProducerConfig.WARM_UP_PERCENT, 20,
            KafkyProducerConfig.MEASURE_RESPONSE_TIME_PERCENT, 5,
            KafkyProducerConfig.MEASURE_RESPONSE_TIME_DELAY, 100,
            KafkyProducerConfig.RECORD_PRODUCERS_PACKAGES, List.of(TemplateRecordProducer.class.getPackage().getName()),
            KafkyProducerConfig.PROPERITES, Map.of(
                    "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
            KafkyProducerConfig.EXPRESSION_FUNCTIONS, Map.of(
                    "RANDOM_UUID", RandomUUIDFunction.class.getName(),
                    "RANDOM_LONG", RandomLongFunction.class.getName(),
                    "SIZE", SizeFunction.class.getName(),
                    "NEW_B3", NewB3Function.class.getName()));

    public static final Map<Object, Object> DEFAULT_REPORT_PROPERTIES = Map.of(
            KafkyReportConfig.JOBS_STATUS_PERIOD, 10_000,
            KafkyReportConfig.SYSTEM_OUT, true,
            KafkyReportConfig.LOG, true);

    private KafkyDefaults() {
        // no instance
    }

}
