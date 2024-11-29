package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import static io.github.cyrilsochor.kafky.api.job.JobState.WARMED;

import io.github.cyrilsochor.kafky.api.job.consumer.AbstractRecordConsumer;
import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.global.PairMatcher;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.util.Map;

public class PairResponseMarker extends AbstractRecordConsumer {

    public static PairResponseMarker of(final Map<Object, Object> cfg, final Runtime runtime, final ConsumerJobStatus jobStatus) throws IOException {
        final String pairMatcherId = PropertiesUtils.getString(cfg, KafkyConsumerConfig.PAIR_MATCHER);
        if (pairMatcherId == null) {
            return null;
        }
        final PairMatcher pairMatcher = runtime.getGlobalComponent(pairMatcherId, PairMatcher.class);

        final String headerKey = PropertiesUtils.getStringRequired(cfg, KafkyConsumerConfig.PAIR_RESPONSE_HEADER);

        return new PairResponseMarker(runtime, pairMatcher, jobStatus, headerKey);
    }

    protected final Runtime runtime;
    protected final PairMatcher pairMatcher;
    protected final ConsumerJobStatus jobStatus;
    protected final String headerKey;

    public PairResponseMarker(
            final Runtime runtime,
            final PairMatcher pairMatcher,
            final ConsumerJobStatus jobStatus,
            final String headerKey) {
        this.runtime = runtime;
        this.pairMatcher = pairMatcher;
        this.jobStatus = jobStatus;
        this.headerKey = headerKey;
    }

    @Override
    public void consume(final ConsumerRecord<Object, Object> consumerRecord) throws Exception {
        final Header header = consumerRecord.headers().lastHeader(headerKey);
        if (header != null) {
            final String key = new String(header.value());
            final boolean warmup = jobStatus.getRuntimeStatus().getMinProducerState().ordinal() <= WARMED.ordinal();
            pairMatcher.addResponse(key, consumerRecord, warmup);
        }

        getChainNext().consume(consumerRecord);
    }

    @Override
    public int getPriority() {
        return 10;
    }

}
