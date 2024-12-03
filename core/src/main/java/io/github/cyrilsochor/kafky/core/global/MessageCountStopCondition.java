package io.github.cyrilsochor.kafky.core.global;

import static io.github.cyrilsochor.kafky.core.config.KafkyConsumerStopCountConfig.MESSAGES_COUNT;

import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;

import java.util.Map;

public class MessageCountStopCondition implements StopCondition {

    public static MessageCountStopCondition of(final Map<Object, Object> cfg) {
        return new MessageCountStopCondition(PropertiesUtils.getLongRequired(cfg, MESSAGES_COUNT));
    }

    protected final long messagesCount;

    public MessageCountStopCondition(final long messagesCount) {
        this.messagesCount = messagesCount;
    }

    @Override
    public Boolean apply(final ConsumerJobStatus status) {
        return status.getConsumedMessagesCount() >= messagesCount;
    }

}