package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordProducer;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class FixedPartitionDecorator extends AbstractRecordProducer {

    protected final Integer partition;

    public static RecordProducer of(final Map<Object, Object> cfg) {
        final Integer partition = PropertiesUtils.getInteger(cfg, KafkyProducerConfig.PARTITION);

        if (partition == null) {
            return null;
        }

        return new FixedPartitionDecorator(partition < 0 ? null : partition);
    }

    @Override
    public int getPriority() {
        return 990;
    }

    protected FixedPartitionDecorator(final Integer partition) {
        this.partition = partition;
    }

    @Override
    public ProducerRecord<Object, Object> produce() throws Exception {
        final ProducerRecord<Object, Object> source = getChainNext().produce();
        return new ProducerRecord<>(
                source.topic(),
                partition,
                source.timestamp(),
                source.key(),
                source.value(),
                source.headers());
    }

    @Override
    public String getComponentInfo() {
        return super.getComponentInfo() + "(" + partition + ")";
    }

}
