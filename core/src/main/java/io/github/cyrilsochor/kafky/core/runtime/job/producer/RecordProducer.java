package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.core.util.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface RecordProducer extends Producer<ProducerRecord<Object, Object>> {

    default String getInfo() {
        return this.getClass().getSimpleName();
    }

}
