package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.core.config.KafkyDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Properties;

public class TransportHeaderCleanerDecorator extends AbstractRecordDecorator implements RecordDecorator {

    public static RecordProducer of(final Properties properties) throws ClassNotFoundException {
        return new TransportHeaderCleanerDecorator();
    }

    @Override
    public int getPriority() {
        return 110;
    }

    @Override
    public ProducerRecord<Object, Object> decorate(final ProducerRecord<Object, Object> record) throws Exception {

        final Headers headers = record.headers();
        if (headers != null) {
            for (final String header : KafkyDefaults.TRANSPORT_HEADERS) {
                headers.remove(header);
            }
        }

        return record;
    }

}
