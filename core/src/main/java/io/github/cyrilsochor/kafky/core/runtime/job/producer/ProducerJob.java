package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.MESSAGES_COUNT;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.go;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.producedAndGo;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.producedAndStop;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertFalse;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNotNull;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getLongRequired;
import static java.lang.String.format;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;

import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.exception.InvalidConfigurationException;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class ProducerJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerJob.class);

    public static ProducerJob of(
            final String name,
            final Properties properties)
            throws Exception {
        LOG.atDebug().setMessage("Producer job {} properties:\n{}")
                .addArgument(name)
                .addArgument((Supplier<String>) () -> properties.entrySet().stream()
                        .map(e -> format("%s: %s", e.getKey(), e.getValue()))
                        .collect(joining("\n")))
                .log();

        final ProducerJob job = new ProducerJob(name, properties);

        job.recordProducer = createRecordProducersChain(properties);

        job.messagesCount = getLongRequired(properties, MESSAGES_COUNT);

        job.records = new LinkedList<>();

        final Path producerLogPath = PropertiesUtils.getPath(properties, KafkyProducerConfig.LOG_FILE);
        if (producerLogPath != null) {
            LOG.info("Writing producer {} log to {}", job.getId(), producerLogPath.toAbsolutePath());
            job.producerLog = Files.newBufferedWriter(producerLogPath);
        }

        job.delay = PropertiesUtils.getDuration(properties, KafkyProducerConfig.DELAY);

        return job;
    }

    protected static RecordProducer createRecordProducersChain(final Properties properties)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final List<String> producersPackages = PropertiesUtils.getNonEmptyListOfStrings(properties, KafkyProducerConfig.RECORD_PRODUCERS_PACKAGE);
        final LinkedList<RecordProducer> producers = producersPackages.stream()
                .peek(producersPackage -> LOG.debug("Finding record producers in package {}", producersPackage))
                .flatMap(producersPackage -> new Reflections(producersPackage, new SubTypesScanner(false))
                        .getSubTypesOf(RecordProducer.class).stream())
                .peek(producerClass -> LOG.debug("Found record producer class {}", producerClass.getName()))
                .filter(producerClass -> !Modifier.isAbstract(producerClass.getModifiers()))
                .map(producerClass -> {
                    try {
                        return (RecordProducer) producerClass.getMethod("of", Properties.class)
                                .invoke(null, properties);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                            | SecurityException e) {
                        throw new InvalidConfigurationException("Invalid factory metod of producer class " + producerClass.getName(), e);
                    }
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.<RecordProducer>comparingInt(p -> p.getPriority()).reversed()
                        .thenComparing(p -> p.getClass().getSimpleName()))
                .peek(p -> LOG.debug("Using record producer with priority {}: {}", p.getPriority(), p))
                .collect(toCollection(LinkedList::new));

        assertFalse(producers.isEmpty(), () -> format("No record producer found in packages %s", producersPackages));

        RecordProducer nextProducer = null;
        for (Iterator<RecordProducer> it = producers.descendingIterator(); it.hasNext();) {
            final RecordProducer producer = it.next();
            if (producer instanceof RecordDecorator decorator) {
                assertNotNull(nextProducer, "Record decorator can't be the last producer");
                decorator.setNextProducer(nextProducer);
            }
            nextProducer = producer;
        }

        return producers.getFirst();
    }

    protected final String name;
    protected final Properties properties;

    protected Long messagesCount;

    protected boolean recordProducerInitialized;
    protected RecordProducer recordProducer;

    protected LinkedList<ProducerRecord<Object, Object>> records;

    protected KafkaProducer<Object, Object> kafkaProducer;

    protected Instant start;

    protected Duration delay;

    protected long messageSeq;

    protected Writer producerLog;

    public ProducerJob(final String name, final Properties properties) {
        this.name = name;
        this.properties = properties;
    }

    @Override
    public String getId() {
        return "producer-" + name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IterationResult prepare() throws Exception {
        if (!recordProducerInitialized) {
            recordProducer.init();
            recordProducerInitialized = true;
            return go();
        }

        final ProducerRecord<Object, Object> message = recordProducer.produce();
        records.add(message);
        return records.size() < messagesCount ? go() : stop();
    }

    @Override
    public IterationResult start() throws Exception {
        start = Instant.now();
        kafkaProducer = new KafkaProducer<>(properties);
        return stop();
    }

    @Override
    public IterationResult run() throws Exception {
        final ProducerRecord<Object, Object> record = records.removeFirst();
        final Future<RecordMetadata> metadataFuture = kafkaProducer.send(record);
        displayMessageInfo(metadataFuture);
        int producedCount = 1;

        messageSeq++;
        if (records.isEmpty()) {
            return producedAndStop(producedCount);
        }

        if (delay != null) {
            parkNanos(delay.toNanos());
        }

        return producedAndGo(producedCount);
    }

    protected void displayMessageInfo(
            final Future<RecordMetadata> metadataFuture) throws InterruptedException, ExecutionException, IOException {
        if (producerLog != null) {
            final RecordMetadata metadata = metadataFuture.get();
            final Long elapsedTime = start == null ? null : Duration.between(start, Instant.now()).toMillis();
            producerLog.append(format("%4dms: message #%04d topic=%s, partition=%02d, offset=%04d, timestamp-utc=%s, thread=%s\n",
                    elapsedTime,
                    messageSeq,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    Instant.ofEpochMilli(metadata.timestamp()),
                    Thread.currentThread().getName()));
            producerLog.flush();
        }
    }

    @Override
    public void finish() throws Exception {
        recordProducer.close();
        if (producerLog != null) {
            producerLog.close();
        }
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    @Override
    public String getInfo() {
        final StringBuffer info = new StringBuffer();

        if (messagesCount != null) {
            appendFieldValue(info, "messages-count", messagesCount);
        }

        int pi = 0;
        for (RecordProducer p = recordProducer; p != null; pi++) {
            if (pi == 0) {
                appendFieldKey(info, "message-producers");
            } else {
                info.append(", ");
            }
            info.append(p.getInfo());

            if (p instanceof RecordDecorator rd) {
                p = rd.getNextProducer();
            } else {
                p = null;
            }
        }

        return info.toString();
    }

    private void appendFieldValue(final StringBuffer stringBuffer, final String fieldName, final Object value) {
        appendFieldKey(stringBuffer, fieldName);
        if (value == null) {
            stringBuffer.append("null");
        } else {
            stringBuffer.append(value.toString());
        }
    }

    private void appendFieldKey(final StringBuffer stringBuffer, final String fieldName) {
        if (!stringBuffer.isEmpty()) {
            stringBuffer.append(", ");
        }

        stringBuffer.append(fieldName);
        stringBuffer.append(": ");
    }

}
