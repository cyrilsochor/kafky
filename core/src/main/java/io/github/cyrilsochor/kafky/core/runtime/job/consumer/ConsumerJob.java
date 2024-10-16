package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.go;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldKey;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldValue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getNonEmptyListOfMaps;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getStringRequired;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.RecordConsumer;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConsumerJob implements Job, ConsumerJobStatus {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerJob.class);

    public static ConsumerJob of(
            final String name,
            final Map<Object, Object> cfg) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        LOG.atDebug().setMessage("Consumer job {} properties:\n{}")
                .addArgument(name)
                .addArgument((Supplier<String>) () -> cfg.entrySet().stream()
                        .map(e -> format("%s: %s", e.getKey(), e.getValue()))
                        .collect(joining("\n")))
                .log();

        final ConsumerJob job = new ConsumerJob(name);

        job.kafkaConsumerProperties = new Properties();
        job.kafkaConsumerProperties.putAll(PropertiesUtils.getMapRequired(cfg, KafkyConsumerConfig.PROPERITES));

        final StopCondition customStopCondition = PropertiesUtils.getObjectInstance(
                cfg,
                StopCondition.class,
                KafkyConsumerConfig.STOP_CONDITION);
        if (customStopCondition != null) {
            assertNull(job.stopCondition, "Stop condition is already defined");
            job.stopCondition = customStopCondition;
        }

        final Long messagesCount = PropertiesUtils.getLong(cfg, KafkyConsumerConfig.MESSAGES_COUNT);
        if (messagesCount != null) {
            assertNull(job.stopCondition, "Stop condition is already defined");
            job.stopCondition = new MessageCountStopCondition(messagesCount);
        }

        if (job.stopCondition == null) {
            job.stopCondition = new NeverStopCondition();
        }

        job.topics = new LinkedList<>();
        job.assignments = new LinkedList<>();
        for (final Map<Object, Object> s : getNonEmptyListOfMaps(cfg, KafkyConsumerConfig.SUBSCRIBES)) {
            final String topic = getStringRequired(s, KafkyConsumerConfig.SUBSCRIBES_TOPIC);
            final Integer partition = PropertiesUtils.getInteger(s, KafkyConsumerConfig.SUBSCRIBES_PARTITION);
            final Long offset = PropertiesUtils.getLong(s, KafkyConsumerConfig.SUBSCRIBES_OFFSET);
            if (partition == null) {
                assertNull(offset, "Partition is required when offset is speciefied");
                job.topics.add(topic);
            } else {
                job.assignments.add(new Assigment(new TopicPartition(topic, partition), offset));
            }
        }
        assertTrue(job.topics.isEmpty() || job.assignments.isEmpty(), "Partition should be specified for all topics or for none");

        final List<String> consumersPackages = PropertiesUtils.getNonEmptyListOfStrings(cfg, KafkyConsumerConfig.RECORD_CONSUMERS_PACKAGES);
        job.recordConsumer = ComponentUtils.findImplementationsChain(
                "record consumer",
                RecordConsumer.class,
                consumersPackages,
                cfg);

        return job;
    }

    protected final String name;
    protected Properties kafkaConsumerProperties;

    protected List<String> topics;
    protected List<Assigment> assignments;

    protected KafkaConsumer<Object, Object> consumer;
    protected ConsumerRecords<Object, Object> records;

    protected RecordConsumer recordConsumer;

    protected long consumedMessagesCount;

    protected Function<ConsumerJobStatus, Boolean> stopCondition;
    protected long pollTimeout = 1000;

    public ConsumerJob(final String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return "consumer-" + name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IterationResult start() throws Exception {
        if (consumer == null) {

            consumer = new KafkaConsumer<>(kafkaConsumerProperties);

            if (isNotEmpty(topics)) {
                consumer.subscribe(topics);
            }
            if (isNotEmpty(assignments)) {
                consumer.assign(assignments.stream()
                        .map(Assigment::topicPartition)
                        .toList());
                assignments.stream()
                        .filter(ass -> ass.offset != null)
                        .forEach(ass -> consumer.seek(ass.topicPartition, ass.offset));
            }

            recordConsumer.init();
        }

        readRecords();

        if (records != null && !records.isEmpty()) {
            return stop();
        }

        if (!consumer.assignment().isEmpty()) {
            return stop();
        }

        return go();
    }

    @Override
    public IterationResult run() throws Exception {

        long iterationStartConsumedMessageCount = consumedMessagesCount;

        readRecords();

        boolean stop = false;
        for (final ConsumerRecord<Object, Object> rec : records) {
            if (!stop) {
                recordConsumer.consume(rec);
                consumedMessagesCount++;
            }
            stop = stopCondition.apply(this);
        }
        records = null;

        return IterationResult.of(
                stop,
                consumedMessagesCount - iterationStartConsumedMessageCount,
                0);
    }

    protected void readRecords() {
        if (records == null || records.isEmpty()) {
            LOG.atTrace()
                    .setMessage("{} assigment: {}, group metadata: {}")
                    .addArgument(this::getName)
                    .addArgument(consumer::assignment)
                    .addArgument(consumer::groupMetadata)
                    .log();
            records = consumer.poll(Duration.of(pollTimeout, ChronoUnit.MILLIS));
        }
    }

    @Override
    public void finish() throws Exception {
        recordConsumer.close();

        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public long getConsumedMessagesCount() {
        return consumedMessagesCount;
    }

    @Override
    public String getInfo() {
        final StringBuilder info = new StringBuilder();

        if (isNotEmpty(topics)) {
            appendFieldValue(info, "topics", topics.stream().collect(joining(", ")));
        }

        if (isNotEmpty(assignments)) {
            appendFieldValue(info, "assigments", assignments.stream()
                    .map(ass -> {
                        final StringBuilder b = new StringBuilder();
                        b.append(ass.topicPartition.topic());
                        b.append(":");
                        b.append(ass.topicPartition);
                        if (ass.offset != null) {
                            b.append(":");
                            b.append(ass.offset);
                        }
                        return b.toString();
                    })
                    .collect(joining(", ")));
        }

        RecordConsumer c = recordConsumer;
        for (int pi = 0; c != null; pi++) {
            if (pi == 0) {
                appendFieldKey(info, "message-consumers");
            } else {
                info.append(", ");
            }
            info.append(c.getComponentInfo());

            c = c.getChainNext();
        }

        return info.toString();
    }

    protected static record Assigment(
            TopicPartition topicPartition,
            Long offset) {

    }

}
