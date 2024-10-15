package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getNonEmptyListOfMaps;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getStringRequired;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.storage.mapper.StorageSerializer;
import io.github.cyrilsochor.kafky.core.storage.text.TextWriter;
import io.github.cyrilsochor.kafky.core.util.Consumer;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class ConsumerJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerJob.class);

    public static ConsumerJob of(
            final String name,
            final Properties properties) {
        LOG.atDebug().setMessage("Consumer job {} properties:\n{}")
                .addArgument(name)
                .addArgument((Supplier<String>) () -> properties.entrySet().stream()
                        .map(e -> format("%s: %s", e.getKey(), e.getValue()))
                        .collect(joining("\n")))
                .log();

        final ConsumerJob job = new ConsumerJob(name, properties);

        job.messagesCount = PropertiesUtils.getLong(properties, KafkyConsumerConfig.MESSAGES_COUNT);

        job.topics = new LinkedList<>();
        job.assignments = new LinkedList<>();
        for (final Map<Object, Object> s : getNonEmptyListOfMaps(properties, KafkyConsumerConfig.SUBSCRIBES)) {
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

        job.outputs = new LinkedList<>();
        final Path outputPath = PropertiesUtils.getPathRequired(properties, KafkyConsumerConfig.OUTPUT_FILE);
        LOG.info("Writing {} consumed messages log to {}", job.getId(), outputPath.toAbsolutePath());
        job.outputs.add(new StorageSerializer(new TextWriter(outputPath)));

        return job;
    }

    protected final String name;
    protected final Properties properties;

    protected List<String> topics;
    protected List<Assigment> assignments;

    protected KafkaConsumer<byte[], byte[]> consumer;

    protected List<Consumer<ConsumerRecord<?, ?>>> outputs;

    protected Long messagesCount;

    protected long consumedMessagesCount;

    public ConsumerJob(final String name, final Properties properties) {
        this.name = name;
        this.properties = properties;
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
        consumer = new KafkaConsumer<>(properties);

        if (isNotEmpty(topics)) {
            consumer.subscribe(topics);
        }
        if (isNotEmpty(assignments)) {
            consumer.assign(assignments.stream()
                    .map(Assigment::topicPartition)
                    .toList());
            assignments.stream()
                    .filter(ass -> ass.offset != null)
                    .forEach(ass -> {
                        consumer.seek(ass.topicPartition, ass.offset);
                    });
        }

        for (Consumer<ConsumerRecord<?, ?>> o : outputs) {
            o.init();
        }

        return stop();
    }

    @Override
    public IterationResult run() throws Exception {
        long iteraationStartConsumedMessageCount = consumedMessagesCount;

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            if (messagesCount == null || consumedMessagesCount < messagesCount) {
                output(record);
                consumedMessagesCount++;
            }
        }

        return IterationResult.of(
                consumedMessagesCount >= messagesCount,
                consumedMessagesCount - iteraationStartConsumedMessageCount,
                0);
    }

    protected void output(final ConsumerRecord<?, ?> record) throws Exception {
        for (Consumer<ConsumerRecord<?, ?>> o : outputs) {
            o.consume(record);
        }
    }

    @Override
    public void finish() throws Exception {
        for (Consumer<ConsumerRecord<?, ?>> o : outputs) {
            o.close();
        }

        if (consumer != null) {
            consumer.close();
        }
    }

    protected record Assigment(
            TopicPartition topicPartition,
            Long offset) {

    }
}