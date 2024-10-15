package io.github.cyrilsochor.kafky.api.job.consumer;

import java.util.function.Function;

public interface StopCondition extends Function<ConsumerJobStatus, Boolean> {

}
