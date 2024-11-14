package io.github.cyrilsochor.kafky.api.job.consumer;

import io.github.cyrilsochor.kafky.api.runtime.RuntimeStatus;

public interface ConsumerJobStatus {

    RuntimeStatus getRuntimeStatus();

    long getConsumedMessagesCount();

}
