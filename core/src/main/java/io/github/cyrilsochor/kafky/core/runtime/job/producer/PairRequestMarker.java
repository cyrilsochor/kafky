package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecordListener;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.pair.PairMatcher;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.util.Map;

public class PairRequestMarker implements ProducedRecordListener {

    public static ProducedRecordListener of(final Map<Object, Object> cfg) throws IOException {
        final String headerKey = PropertiesUtils.getString(cfg, KafkyProducerConfig.PAIR_REQUEST_HEADER);
        if (headerKey == null) {
            return null;
        }

        return new PairRequestMarker(headerKey);
    }

    protected final String headerKey;

    public PairRequestMarker(final String headerKey) {
        this.headerKey = headerKey;
    }

    @Override
    public void init() throws Exception {
        PairMatcher.registerRequestProducer(this);
    }

    @Override
    public void close() throws Exception {
        PairMatcher.unregisterRequestProducer(this);
    }

    @Override
    public void consume(final ProducedRecord producedRecord) throws Exception {
        final Header header = producedRecord.record().headers().lastHeader(headerKey);
        if (header != null) {
            final String key = new String(header.value());
            PairMatcher.addRequest(key, producedRecord);
        }
    }

}
