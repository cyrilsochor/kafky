package io.github.cyrilsochor.kafky.core.config;

public class KafkyProducerConfig {

    public static final String PROPERITES = "properties";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String INPUT_PATH = "template-file";
    public static final String MESSAGES_COUNT = "messages-count";
    public static final String WARM_UP_PERCENT = "warm-up-percent";
    public static final String MEASURE_RESPONSE_TIME_PERCENT = "response-time-percent";
    public static final String GENERATOR_VALUE_CLASS = "generator-value-class";
    public static final String LOG_FILE = "log-file";
    public static final String WARM_UP_DELAY = "warm-up-delay";
    public static final String MEASURE_RESPONSE_TIME_DELAY = "measure-response-time-delay";
    public static final String MEASURE_THROUGHPUT_DELAY = "measure-throughput-delay";
    public static final String RECORD_PRODUCERS_PACKAGES = "record-producers-packages";
    public static final String DECORATE_HEADERS = "headers";
    public static final String DECORATE_VALUE = "value";
    public static final String EXPRESSION_FUNCTIONS = "expression-functions";
    public static final String EXPRESSION_CONSTANT_ARRAY_FILES = "expression-constant-array-files";
    public static final String PAIR_MATCHER = "pair-matcher";
    public static final String PAIR_REQUEST_HEADER = "pair-request-header";

    private KafkyProducerConfig() {
        //no instance
    }

}
