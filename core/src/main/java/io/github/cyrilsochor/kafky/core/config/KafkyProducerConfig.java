package io.github.cyrilsochor.kafky.core.config;

public class KafkyProducerConfig {

    public static final String PREFIX = "kafky.";

    public static final String TOPIC = PREFIX + "topic";
    public static final String INPUT_PATH = PREFIX + "template.file";
    public static final String MESSAGES_COUNT = PREFIX + "messages.count";
    public static final String GENERATOR_VALUE_CLASS = PREFIX + "generator.value.class";
    public static final String LOG_FILE = PREFIX + "log.file";
    public static final String DELAY = PREFIX + "delay";
    public static final String RECORD_PRODUCERS_PACKAGE = PREFIX + "record.producers.package";

}
