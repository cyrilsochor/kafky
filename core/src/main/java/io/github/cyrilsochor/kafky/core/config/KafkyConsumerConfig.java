package io.github.cyrilsochor.kafky.core.config;

public class KafkyConsumerConfig {

    public static final String PROPERITES = "properties";
    public static final String OUTPUT_FILE = "output-file";
    public static final String SUBSCRIBES = "subscribes";
    public static final String SUBSCRIBES_TOPIC = "topic";
    public static final String SUBSCRIBES_PARTITION = "partition";
    public static final String SUBSCRIBES_OFFSET = "offset";
    public static final String SKIP_WARM_UP = "skip-warm-up";
    public static final String STOP_CONDITION = "stop-condition";
    public static final String RECORD_CONSUMERS_PACKAGES = "record-consumers-packages";
    public static final String PAIR_MATCHER = "pair-matcher";
    public static final String PAIR_RESPONSE_HEADER = "pair-response-header";
    public static final String PASSER_BY_DETECT = "passer-by-detect";
    public static final String PROCESSOR_GROUP = "processor-group";

    private KafkyConsumerConfig() {
        //no instance
    }

}
