package io.github.cyrilsochor.kafky.api.job.consumer;

import io.github.cyrilsochor.kafky.api.component.AbstractChainComponent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class AbstractRecordConsumer extends AbstractChainComponent<RecordConsumer> implements RecordConsumer {

    @Override
    public void consume(final ConsumerRecord<Object, Object> consumerRecord) throws Exception {
        final RecordConsumer n = getChainNext();
        if (n != null) {
            n.consume(consumerRecord);
        }
    }

}
