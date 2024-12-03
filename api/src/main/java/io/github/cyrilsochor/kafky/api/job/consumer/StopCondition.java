package io.github.cyrilsochor.kafky.api.job.consumer;

import io.github.cyrilsochor.kafky.api.component.Component;

import java.util.function.Function;

public interface StopCondition extends Function<ConsumerJobStatus, Boolean>, Component {

}
