package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;

public class MessageCountStopCondition implements StopCondition {

    protected final long messagesCount;

    public MessageCountStopCondition(final Long messagesCount) {
        this.messagesCount = messagesCount;
    }

    @Override
    public Boolean apply(final ConsumerJobStatus status) {
        return status.getConsumedMessagesCount() >= messagesCount;
    }

}