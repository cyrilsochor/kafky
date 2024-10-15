package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.util.Assert.assertFalse;

import io.github.cyrilsochor.kafky.api.job.Producer;
import io.github.cyrilsochor.kafky.api.job.producer.AbstractRecordProducer;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.storage.mapper.StorageDeserializer;
import io.github.cyrilsochor.kafky.core.storage.model.Message;
import io.github.cyrilsochor.kafky.core.storage.text.TextReader;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TemplateRecordProducer extends AbstractRecordProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateRecordProducer.class);

    public static RecordProducer of(final Map<Object, Object> cfg) throws Exception {
        final Path templatePath = PropertiesUtils.getPath(cfg, KafkyProducerConfig.INPUT_PATH);
        if (templatePath == null) {
            return null;
        }

        return new TemplateRecordProducer(templatePath);
    }

    protected final Producer<ConsumerRecord<Object, Object>> templateProducer;
    protected final List<ConsumerRecord<Object, Object>> templates;
    protected int lastTemplateIndex;
    protected final GenericData genericData = new GenericData();

    protected TemplateRecordProducer(final Path templatePath) throws Exception {
        super();
        final Producer<Message> messageProducer = new TextReader(templatePath);
        templateProducer = new StorageDeserializer(messageProducer);
        templateProducer.init();
        templates = new LinkedList<>();
        lastTemplateIndex = -1;
    }

    @Override
    public int getPriority() {
        return Integer.MIN_VALUE + 100;
    }

    @Override
    public ProducerRecord<Object, Object> produce() throws Exception {
        LOG.trace("Genering message from template");

        final ConsumerRecord<?, ?> template = nextTemplate();
        final String topic = template.topic();
        final int partition = template.partition();
        final Object key = generateObject(template.key());
        final Object value = generateObject(template.value());
        final Long timestamp = null;
        final Iterable<Header> headers = template.headers();
        final ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                topic,
                partition,
                timestamp,
                key,
                value,
                headers);

        LOG.debug("Generated record: {}", producerRecord);
        return producerRecord;
    }

    protected Object generateObject(final Object templateObject) {
        final Object value = templateObject;

        if (value != null && value instanceof GenericRecord gr) {
            return genericData.deepCopy(gr.getSchema(), gr);
        } else {
            return value;
        }
    }

    @SuppressWarnings("java:S1452")
    protected ConsumerRecord<Object, Object> nextTemplate() throws Exception {
        if (lastTemplateIndex < 0) {
            final ConsumerRecord<Object, Object> template = templateProducer.produce();
            if (template != null) {
                templates.add(template);
                return template;
            } else {
                assertFalse(templates.isEmpty(), "There is no template");
                lastTemplateIndex = 0;
                return templates.get(lastTemplateIndex);
            }
        } else {
            if (++lastTemplateIndex >= templates.size()) {
                lastTemplateIndex = 0;
            }
            return templates.get(lastTemplateIndex);
        }
    }

    @Override
    public void close() throws Exception {
        templateProducer.close();
    }

}
