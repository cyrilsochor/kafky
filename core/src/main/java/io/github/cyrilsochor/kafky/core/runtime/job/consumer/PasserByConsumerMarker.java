package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.consumer.AbstractRecordConsumer;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.global.PairMatcher;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;

public class PasserByConsumerMarker extends AbstractRecordConsumer {

    public static PasserByConsumerMarker of(final Map<Object, Object> cfg, final Runtime runtime) throws IOException {
        final String passerByDetect = PropertiesUtils.getString(cfg, KafkyConsumerConfig.PASSER_BY_DETECT);
        if (passerByDetect == null) {
            return null;
        }

        final PairMatcher pairMatcher = runtime.getGlobalComponent(passerByDetect, PairMatcher.class);

        return new PasserByConsumerMarker(pairMatcher);
    }

    protected final PairMatcher pairMatcher;

    public PasserByConsumerMarker(final PairMatcher pairMatcher) {
        this.pairMatcher = pairMatcher;
    }

    @Override
    public void consume(final ConsumerRecord<Object, Object> consumerRecord) throws Exception {
        pairMatcher.addIngoing(consumerRecord);
    }

}
