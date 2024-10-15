package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordDecorator;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class FixedTopicDecorator extends AbstractRecordDecorator {

    protected final String topic;

    public static RecordProducer of(final Map<Object, Object> cfg) {
        final String topic = PropertiesUtils.getString(cfg, KafkyProducerConfig.TOPIC);

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
    public ProducerRecord<Object, Object> decorate(final ProducerRecord<Object, Object> rec) throws Exception {
        return new ProducerRecord<>(
                topic,
                rec.partition(),
                rec.timestamp(),
                rec.key(),
                rec.value(),
                rec.headers());
    }

    @Override
    public String getInfo() {
        return super.getInfo() + "(" + topic + ")";
    }

}
