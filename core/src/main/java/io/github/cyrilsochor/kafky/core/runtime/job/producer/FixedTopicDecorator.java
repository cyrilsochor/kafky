package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordDecorator;
import io.github.cyrilsochor.kafky.api.job.producer.RecordDecorator;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FixedTopicDecorator extends AbstractRecordDecorator implements RecordDecorator {

    protected final String topic;

    public static RecordProducer of(final Properties properties) throws ClassNotFoundException {
        final String topic = PropertiesUtils.getString(properties, KafkyProducerConfig.TOPIC);

        if (topic == null) {
            return null;
        }

        return new FixedTopicDecorator(topic);
    }

    @Override
    public int getPriority() {
        return 1000;
    }

    protected FixedTopicDecorator(final String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<Object, Object> decorate(final ProducerRecord<Object, Object> record) throws Exception {
        return new ProducerRecord<Object, Object>(
                topic,
                record.partition(),
                record.timestamp(),
                record.key(),
                record.value(),
                record.headers());
    }

    @Override
    public String getInfo() {
        return super.getInfo() + "(" + topic + ")";
    }

}
