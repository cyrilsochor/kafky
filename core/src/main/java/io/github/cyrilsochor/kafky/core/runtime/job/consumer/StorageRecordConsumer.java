package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.consumer.AbstractRecordConsumer;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.storage.mapper.StorageSerializer;
import io.github.cyrilsochor.kafky.core.storage.text.TextWriter;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class StorageRecordConsumer extends AbstractRecordConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRecordConsumer.class);

    public static StorageRecordConsumer of(final Map<Object, Object> cfg) throws IOException {
        final Path outputPath = PropertiesUtils.getPath(cfg, KafkyConsumerConfig.OUTPUT_FILE);
        if (outputPath == null) {
            return null;
        }

        LOG.info("Writing consumed messages log to {}", outputPath.toAbsolutePath());

        return new StorageRecordConsumer(new StorageSerializer(new TextWriter(outputPath)));
    }

    final StorageSerializer storageSerializer;

    public StorageRecordConsumer(final StorageSerializer storageSerializer) {
        this.storageSerializer = storageSerializer;
    }

    @Override
    public void init() throws Exception {
        LOG.debug("Init start");
        storageSerializer.init();
        LOG.debug("Init finish");
        super.init();
    }

    @Override
    public void consume(final ConsumerRecord<Object, Object> consumerRecord) throws Exception {
        LOG.debug("Consume start: {}", consumerRecord);
        storageSerializer.consume(consumerRecord);
        LOG.debug("Consume finish");
        super.consume(consumerRecord);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Close start");
        storageSerializer.close();
        LOG.debug("Close finish");
        super.close();
    }

}
