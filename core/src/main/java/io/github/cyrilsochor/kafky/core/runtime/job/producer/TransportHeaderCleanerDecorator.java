package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordDecorator;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Map;

public class TransportHeaderCleanerDecorator extends AbstractRecordDecorator {

    public static RecordProducer of(final Map<Object, Object> cfg) {
        return new TransportHeaderCleanerDecorator();
    }

    @Override
    public int getPriority() {
        return 110;
    }

    @Override
    public ProducerRecord<Object, Object> decorate(final ProducerRecord<Object, Object> rec) throws Exception {

        final Headers headers = rec.headers();
        if (headers != null) {
            for (final String header : KafkyDefaults.TRANSPORT_HEADERS) {
                headers.remove(header);
            }
        }

        return rec;
    }

}
