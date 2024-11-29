package io.github.cyrilsochor.kafky.core.global;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface PairMatcher {

    void addProducedRequest(String pairKey, ProducedRecord record);

    void addResponse(String pairKey, ConsumerRecord<Object, Object> record, boolean warmup);

    void addIngoing(ConsumerRecord<Object, Object> inputMessage);

    boolean isAllPaired();

}
