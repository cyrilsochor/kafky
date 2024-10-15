package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.MESSAGES_COUNT;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.go;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.producedAndGo;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.producedAndStop;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldKey;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldValue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getLongRequired;
import static java.lang.String.format;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.stream.Collectors.joining;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecordListener;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public class ProducerJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerJob.class);

    public static ProducerJob of(
            final String name,
            final Map<Object, Object> cfg) throws IOException {
        LOG.atDebug().setMessage("Producer job {} properties:\n{}")
                .addArgument(name)
                .addArgument((Supplier<String>) () -> cfg.entrySet().stream()
                        .map(e -> format("%s: %s", e.getKey(), e.getValue()))
                        .collect(joining("\n")))
                .log();

        final ProducerJob job = new ProducerJob(name);

        job.kafkaProducerProperties = new Properties();
        job.kafkaProducerProperties.putAll(PropertiesUtils.getMapRequired(cfg, KafkyProducerConfig.PROPERITES));

        final List<String> producersPackages = PropertiesUtils.getNonEmptyListOfStrings(cfg, KafkyProducerConfig.RECORD_PRODUCERS_PACKAGES);
        job.recordProducer = ComponentUtils.findImplementationsChain(
                "record producer",
                RecordProducer.class,
                producersPackages,
                cfg);
        job.listeners = ComponentUtils.findImplementations(
                "record producer",
                ProducedRecordListener.class,
                producersPackages,
                cfg);

        job.messagesCount = getLongRequired(cfg, MESSAGES_COUNT);

        job.delay = PropertiesUtils.getDuration(cfg, KafkyProducerConfig.DELAY);

        return job;
    }

    protected final String name;
    protected Properties kafkaProducerProperties;

    protected Long messagesCount;

    protected boolean initialized;
    protected RecordProducer recordProducer;


    protected KafkaProducer<Object, Object> kafkaProducer;

    protected Duration delay;

    protected long messageSendSeq;
    protected long messageLogSeq;

    protected List<ProducedRecordListener> listeners;

    protected LinkedList<ProducerRecord<Object, Object>> records = new LinkedList<>();

    public ProducerJob(final String name) {
        this.name = name;
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
        if (!initialized) {

            recordProducer.init();
            for (ProducedRecordListener listener : listeners) {
                listener.init();
            }

            initialized = true;
            return go();
        }

        final ProducerRecord<Object, Object> message = recordProducer.produce();
        records.add(message);
        return records.size() < messagesCount ? go() : stop();
    }

    @Override
    public IterationResult start() throws Exception {
        kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        return stop();
    }

    @Override
    public IterationResult run() throws Exception {
        if (delay != null) {
            parkNanos(delay.toNanos());
        }

        final ProducerRecord<Object, Object> rec = records.removeFirst();
        messageSendSeq++;
        kafkaProducer.send(rec, (Callback) (metadata, exception) -> notifyListeners(messageLogSeq, rec, metadata, exception));

        int producedCount = 1;

        if (records.isEmpty()) {
            return producedAndStop(producedCount);
        }

        return producedAndGo(producedCount);
    }

    protected void notifyListeners(
            final long messageSeq,
            final ProducerRecord<Object, Object> producerRecord,
            final RecordMetadata metadata,
            final Exception sendException) {
        messageLogSeq++;
        final ProducedRecord produced = new ProducedRecord(messageSeq, producerRecord, metadata, sendException);
        for (ProducedRecordListener listener : listeners) {
            try {
                listener.consume(produced);
            } catch (Exception e) {
                LOG.error("{} notification error", listener.getClass().getName(), e);
            }
        }
    }

    @Override
    public void finish() throws Exception {
        recordProducer.close();
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }

        while (messageLogSeq != messageSendSeq) {
            LOG.debug("Waiting for metadata ({}!={})", messageLogSeq, messageSendSeq);
            LockSupport.parkNanos(1_000_000);
        }

        for (ProducedRecordListener listener : listeners) {
            listener.close();
        }
    }

    @Override
    public String getInfo() {
        final StringBuilder info = new StringBuilder();

        if (messagesCount != null) {
            appendFieldValue(info, "messages-count", messagesCount);
        }

        RecordProducer p = recordProducer;
        for (int pi = 0; p != null; pi++) {
            if (pi == 0) {
                appendFieldKey(info, "message-producers");
            } else {
                info.append(", ");
            }
            info.append(p.getComponentInfo());

            p = p.getChainNext();
        }

        return info.toString();
    }

}
