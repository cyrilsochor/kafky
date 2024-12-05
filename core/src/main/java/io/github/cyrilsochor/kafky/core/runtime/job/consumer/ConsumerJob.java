package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.go;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldKey;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldValue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getNonEmptyListOfMaps;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getStringRequired;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.api.job.consumer.RecordConsumer;
import io.github.cyrilsochor.kafky.api.job.consumer.StopCondition;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import io.github.cyrilsochor.kafky.core.runtime.job.AbstractJob;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils.ImplementationParameter;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.commons.lang3.RandomStringUtils;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class ConsumerJob extends AbstractJob implements Job, ConsumerJobStatus {


    private static final Logger LOG = LoggerFactory.getLogger(ConsumerJob.class);

    protected static final String PROP_GROUP_ID = "group.id";
    protected static final String GENERATED_GROUP_ID_PREFIX = "kafky";
    protected static final int GENERATED_GROUP_ID_RANDOM_LENGTH = 20;

    public static ConsumerJob of(
            final Runtime runtime,
            final String name,
            final Map<Object, Object> cfg) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        final ConsumerJob job = new ConsumerJob(runtime, name);

        job.kafkaConsumerProperties = new Properties();
        job.kafkaConsumerProperties.putAll(PropertiesUtils.getMapRequired(cfg, KafkyConsumerConfig.PROPERITES));
        if (!job.kafkaConsumerProperties.containsKey(PROP_GROUP_ID)) {
            job.kafkaConsumerProperties.put(PROP_GROUP_ID,
                    GENERATED_GROUP_ID_PREFIX + RandomStringUtils.insecure().next(GENERATED_GROUP_ID_RANDOM_LENGTH, true, true));
        }

        final String customStopConditionId = PropertiesUtils.getString(cfg, KafkyConsumerConfig.STOP_CONDITION);
        job.stopCondition = customStopConditionId != null ? runtime.getGlobalComponent(customStopConditionId, StopCondition.class)
                : new NeverStopCondition();

        job.skipWarmUp = PropertiesUtils.getBooleanRequired(cfg, KafkyConsumerConfig.SKIP_WARM_UP);

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
                List.of(
                        new ImplementationParameter(Map.class, cfg),
                        new ImplementationParameter(ConsumerJobStatus.class, job),
                        new ImplementationParameter(Runtime.class, runtime)));

        return job;
    }

    protected Properties kafkaConsumerProperties;

    protected List<String> topics;
    protected List<Assigment> assignments;

    protected KafkaConsumer<Object, Object> consumer;
    protected LinkedList<ConsumerRecord<Object, Object>> records = new LinkedList<>();

    protected RecordConsumer recordConsumer;

    protected long consumedMessagesCount;

    protected boolean skipWarmUp;
    protected Function<ConsumerJobStatus, Boolean> stopCondition;
    protected long pollTimeout = 1000;

    public ConsumerJob(final Runtime runtime, final String name) {
        super(runtime, "consumer", name);
    }

    @Override
    public IterationResult start() throws Exception {
        if (consumer == null) {

            recordConsumer.init();

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

        }

        readRecords();

        if (!records.isEmpty()) {
            return stop();
        }

        if (!consumer.assignment().isEmpty()) {
            return stop();
        }

        return go();
    }

    @Override
    public boolean skipWarmUp() {
        return skipWarmUp;
    }

    @Override
    public IterationResult warmUp() throws Exception {
        return iterate();
    }

    @Override
    public IterationResult run() throws Exception {
        return iterate();
    }

    protected IterationResult iterate() throws Exception {
        long iterationStartConsumedMessageCount = consumedMessagesCount;

        boolean stop = stopCondition.apply(this);
        if (stop) {
            return IterationResult.stop();
        }

        readRecords();

        while (!stop) {
            final ConsumerRecord<Object, Object> rec = records.poll();
            if (rec == null) {
                break;
            }
            recordConsumer.consume(rec);
            consumedMessagesCount++;
            stop = stopCondition.apply(this);
        }

        return IterationResult.of(
                stop,
                consumedMessagesCount - iterationStartConsumedMessageCount,
                0);
    }

    protected void readRecords() {
        if (records.isEmpty()) {
            LOG.atTrace()
                    .setMessage("{} assigment: {}, group metadata: {}")
                    .addArgument(this::getName)
                    .addArgument(consumer::assignment)
                    .addArgument(consumer::groupMetadata)
                    .log();
            final ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.of(pollTimeout, ChronoUnit.MILLIS));
            LOG.atTrace()
                    .setMessage("{} polled: {} records")
                    .addArgument(this::getName)
                    .addArgument(consumerRecords.count())
                    .log();

            consumerRecords.forEach(records::add);
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
    public void shutdownHook() {
        recordConsumer.shutdownHook();
    }

    @Override
    public Set<String> getConsumedTopics() {
        return Stream.concat(topics.stream(),
                assignments.stream().map(a -> a.topicPartition.topic()))
                .collect(toSet());
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
