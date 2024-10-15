package io.github.cyrilsochor.kafky.api.job.producer;

import io.github.cyrilsochor.kafky.api.component.ChainComponent;
import io.github.cyrilsochor.kafky.api.job.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface RecordProducer extends Producer<ProducerRecord<Object, Object>>, ChainComponent<RecordProducer> {

}
