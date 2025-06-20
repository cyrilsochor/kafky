package io.github.cyrilsochor.kafky.core.global;

import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;
import io.github.cyrilsochor.kafky.core.runtime.KafkyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PairedStopCondition implements StopCondition {

    private static final Logger LOG = LoggerFactory.getLogger(PairedStopCondition.class);

    protected final KafkyRuntime runtime;

    public static PairedStopCondition of(final KafkyRuntime runtime) {
        return new PairedStopCondition(runtime);
    }

    protected PairedStopCondition(KafkyRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public Boolean apply(final ConsumerJobStatus status) {
        for (final PairMatcher pairMatcher : runtime.getGlobalComponentsByType(PairMatcher.class)) {
            if (!pairMatcher.areAllRequestsPaired()) {
                LOG.debug("Stop FALSE (isAllPaired FALSE)");
                return false;
            }
        }

        // check all producers are complete
        final JobState minProducerState = runtime.getMinProducerState();
        final boolean stop = minProducerState.ordinal() > status.getState().ordinal();
            
        LOG.debug("Stop {} (minProducerState {})", stop, minProducerState);
        return stop;
    }

}
