package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import io.github.cyrilsochor.kafky.api.job.consumer.AbstractRecordConsumer;
import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.global.PairMatcher;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

public class PasserByConsumerMarker extends AbstractRecordConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PasserByConsumerMarker.class);

    public static PasserByConsumerMarker of(
            final Map<Object, Object> cfg,
            final Runtime runtime,
            final ConsumerJobStatus consumerJobStatus) throws IOException {
        final String passerByDetect = PropertiesUtils.getString(cfg, KafkyConsumerConfig.PASSER_BY_DETECT);
        if (passerByDetect == null) {
            return null;
        }

        final PairMatcher pairMatcher = runtime.getGlobalComponent(passerByDetect, PairMatcher.class);

        final String processorGroup = PropertiesUtils.getString(cfg, KafkyConsumerConfig.PROCESSOR_GROUP);
        final Properties adminClientProperties;
        if (processorGroup == null) {
            adminClientProperties = null;
        } else {
            adminClientProperties = new Properties();
            adminClientProperties.putAll(PropertiesUtils.getMapRequired(cfg, KafkyConsumerConfig.PROPERITES));
        }

        return new PasserByConsumerMarker(pairMatcher, consumerJobStatus, processorGroup, adminClientProperties);
    }

    protected final PairMatcher pairMatcher;
    protected final ConsumerJobStatus consumerJobStatus;

    // nullable
    protected final String processorGroup;

    // nullable
    protected final Properties adminClientProperties;

    public PasserByConsumerMarker(
            final PairMatcher pairMatcher,
            final ConsumerJobStatus consumerJobStatus,
            final String processorGroup,
            final Properties adminClientProperties) {
        this.pairMatcher = pairMatcher;
        this.consumerJobStatus = consumerJobStatus;
        this.processorGroup = processorGroup;
        this.adminClientProperties = adminClientProperties;
    }

    @Override
    public void init() throws Exception {
        forEachProcessorOffset((BiConsumer<? super TopicPartition, ? super OffsetAndMetadata>) (tp, om) -> {
            pairMatcher.setProcesserStartOffset(tp.topic(), tp.partition(), om.offset());
        });
        super.init();
    }

    @Override
    public void consume(final ConsumerRecord<Object, Object> consumerRecord) throws Exception {
        pairMatcher.addIngoing(consumerRecord);
        getChainNext().consume(consumerRecord);
    }

    @Override
    public void close() throws Exception {
        forEachProcessorOffset((BiConsumer<? super TopicPartition, ? super OffsetAndMetadata>) (tp, om) -> {
            pairMatcher.setProcesserFinishOffset(tp.topic(), tp.partition(), om.offset());
        });
        super.close();
    }

    protected void forEachProcessorOffset(final BiConsumer<? super TopicPartition, ? super OffsetAndMetadata> action)
            throws InterruptedException, ExecutionException {
        if (processorGroup != null) {
            LOG.atDebug().setMessage("Retrieving processor offsets for group {}")
                    .addArgument(processorGroup)
                    .log();
            final Set<String> consumedTopics = consumerJobStatus.getConsumedTopics();
            try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
                adminClient
                        .listConsumerGroupOffsets(processorGroup)
                        .partitionsToOffsetAndMetadata()
                        .get()
                        .entrySet()
                        .stream()
                        .filter(e -> consumedTopics.contains(e.getKey().topic()))
                        .forEach(e -> action.accept(e.getKey(), e.getValue()));
            }
        }
    }

}
