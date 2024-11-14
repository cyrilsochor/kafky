package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;
import io.github.cyrilsochor.kafky.core.pair.PairMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PairedStopCondition implements StopCondition {

    private static final Logger LOG = LoggerFactory.getLogger(PairedStopCondition.class);

    public PairedStopCondition(final Map<Object, Object> cfg) {
    }

    @Override
    public Boolean apply(final ConsumerJobStatus status) {
        if (!PairMatcher.isAllPaired()) {
            LOG.debug("Stop FALSE (isAllPaired FALSE)");
            return false;
        }

        final JobState minProducerState = status.getRuntimeStatus().getMinProducerState();
        final JobState maxJobState = status.getRuntimeStatus().getMaxJobState();
        final boolean stop;
        if (maxJobState.ordinal() <= JobState.WARMED.ordinal()) {
            // producers may be in states: WARM_UP, WARMED
            stop = minProducerState.ordinal() >= JobState.WARMED.ordinal();
        } else {
            // producers may be in states: RUNNING, SUCCESS, FAILURE
            stop = minProducerState.ordinal() > JobState.RUNNING.ordinal();
        }

        LOG.debug("Stop {} (maxJobState {}, minProducerState {})", stop, maxJobState, minProducerState);
        return stop;
    }

}