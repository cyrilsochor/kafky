package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;
import io.github.cyrilsochor.kafky.core.pair.PairMatcher;

import java.util.Map;

public class PairedStopCondition implements StopCondition {

    public PairedStopCondition(final Map<Object, Object> cfg) {
    }

    @Override
    public Boolean apply(final ConsumerJobStatus status) {
        return PairMatcher.isAllProduced() && PairMatcher.isAllPaired();
    }

}