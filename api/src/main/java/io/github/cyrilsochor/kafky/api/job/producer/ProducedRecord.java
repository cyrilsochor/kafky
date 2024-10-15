package io.github.cyrilsochor.kafky.api.job.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public record ProducedRecord(
        long sequenceNumber,
        ProducerRecord<Object, Object> record,
        RecordMetadata metadata,
        Exception sendException) {
}
