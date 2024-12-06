package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecordListener;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.global.PairMatcher;
import io.github.cyrilsochor.kafky.core.runtime.KafkyRuntime;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.util.Map;

public class PairRequestMarker implements ProducedRecordListener {

    public static ProducedRecordListener of(final Map<Object, Object> cfg, final KafkyRuntime runtime) throws IOException {
        final String pairMatcherId = PropertiesUtils.getString(cfg, KafkyProducerConfig.PAIR_MATCHER);
        if (pairMatcherId == null) {
            return null;
        }
        final PairMatcher pairMatcher = runtime.getGlobalComponent(pairMatcherId, PairMatcher.class);

        final String headerKey = PropertiesUtils.getStringRequired(cfg, KafkyProducerConfig.PAIR_REQUEST_HEADER);

        return new PairRequestMarker(pairMatcher, headerKey);
    }

    protected final PairMatcher pairMatcher;
    protected final String headerKey;

    public PairRequestMarker(final PairMatcher pairMatcher, final String headerKey) {
        this.pairMatcher = pairMatcher;
        this.headerKey = headerKey;
    }

    @Override
    public void consume(final ProducedRecord producedRecord) throws Exception {
        final Header header = producedRecord.record().headers().lastHeader(headerKey);
        if (header != null) {
            final String key = new String(header.value());
            pairMatcher.addProducedRequest(key, producedRecord);
        }
    }

}
