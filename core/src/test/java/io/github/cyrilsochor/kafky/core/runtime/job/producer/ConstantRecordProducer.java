package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ConstantRecordProducer extends AbstractRecordProducer {

    final ProducerRecord<Object, Object> record;

    public ConstantRecordProducer(ProducerRecord<Object, Object> record) {
        super();
        this.record = record;
    }

    @Override
    public ProducerRecord<Object, Object> produce() throws Exception {
        return record;
    }

}
