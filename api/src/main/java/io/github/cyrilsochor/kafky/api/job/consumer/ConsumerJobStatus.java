package io.github.cyrilsochor.kafky.api.job.consumer;

import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.api.runtime.RuntimeStatus;

public interface ConsumerJobStatus {

    JobState getState();

    RuntimeStatus getRuntimeStatus();

    long getConsumedMessagesCount();

}
