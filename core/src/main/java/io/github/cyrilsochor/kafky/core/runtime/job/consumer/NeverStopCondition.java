package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;

public class NeverStopCondition implements StopCondition {

    @Override
    public Boolean apply(final ConsumerJobStatus status) {
        return false;
    }

}
