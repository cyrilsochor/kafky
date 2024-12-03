package io.github.cyrilsochor.kafky.core.storage.mapper;

import static java.util.Collections.singletonList;

import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.api.job.Consumer;
import io.github.cyrilsochor.kafky.core.storage.model.Header;
import io.github.cyrilsochor.kafky.core.storage.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.RecordBatch;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class StorageSerializer implements Consumer<ConsumerRecord<Object, Object>>, Component {

    protected final List<Consumer<Message>> consumers;

    public StorageSerializer(Consumer<Message> consumer) {
        this(singletonList(consumer));
    }

    public StorageSerializer(List<Consumer<Message>> consumers) {
        this.consumers = consumers;
    }

    @Override
    public void init() throws Exception {
        for (Consumer<Message> consumer : consumers) {
            consumer.init();
        }
    }

    @Override
    public void close() throws Exception {
        for (Consumer<Message> consumer : consumers) {
            consumer.close();
        }
    }

    @Override
    public void consume(final ConsumerRecord<Object, Object> input) throws Exception {
        final Message message = new Message(
                input.topic(),
                input.partition(),
                input.offset(),
                serializeTimestamp(input.timestamp()),
                serializeHeaders(input.headers()),
                input.key(),
                input.value());
        for (Consumer<Message> consumer : consumers) {
            consumer.consume(message);
        }
    }

    protected LocalDateTime serializeTimestamp(final long timestamp) {
        return timestamp == RecordBatch.NO_TIMESTAMP ? null
                : LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
    }

    protected Collection<Header> serializeHeaders(final Headers inputHeaders) {
        if (inputHeaders == null || !inputHeaders.iterator().hasNext()) {
            return null;
        }

        final List<Header> result = new LinkedList<>();

        for (org.apache.kafka.common.header.Header inputHeader : inputHeaders) {
            final Header header = new Header(
                    inputHeader.key(),
                    new String(inputHeader.value()));
            result.add(header);
        }

        return result;
    }

}
