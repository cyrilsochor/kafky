package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordProducer;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class FixedTopicDecorator extends AbstractRecordProducer {

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
        return 900;
    }

    protected FixedTopicDecorator(final String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<Object, Object> produce() throws Exception {
        final ProducerRecord<Object, Object> source = getChainNext().produce();
        return new ProducerRecord<>(
                topic,
                source.partition(),
                source.timestamp(),
                source.key(),
                source.value(),
                source.headers());
    }

    @Override
    public String getComponentInfo() {
        return super.getComponentInfo() + "(" + topic + ")";
    }

}
