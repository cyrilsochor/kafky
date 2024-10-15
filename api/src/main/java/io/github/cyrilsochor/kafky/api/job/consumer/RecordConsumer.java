package io.github.cyrilsochor.kafky.api.job.consumer;

import io.github.cyrilsochor.kafky.api.component.ChainComponent;
import io.github.cyrilsochor.kafky.api.job.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordConsumer extends Consumer<ConsumerRecord<Object, Object>>, ChainComponent<RecordConsumer> {

}
