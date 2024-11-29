package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.MESSAGES_COUNT;
import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.WARM_UP_PERCENT;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.go;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.produced;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldKey;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldValue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getLong;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getLongRequired;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecordListener;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import io.github.cyrilsochor.kafky.core.runtime.job.AbstractJob;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils.ImplementationParameter;
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

public class ProducerJob extends AbstractJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerJob.class);

    public static ProducerJob of(
            final Runtime runtime,
            final String name,
            final Map<Object, Object> cfg) throws IOException {
        final ProducerJob job = new ProducerJob(runtime, name);

        job.kafkaProducerProperties = new Properties();
        job.kafkaProducerProperties.putAll(PropertiesUtils.getMapRequired(cfg, KafkyProducerConfig.PROPERITES));

        final List<String> producersPackages = PropertiesUtils.getNonEmptyListOfStrings(cfg, KafkyProducerConfig.RECORD_PRODUCERS_PACKAGES);
        final List<ImplementationParameter> parameters = List.of(
                new ImplementationParameter(Map.class, cfg),
                new ImplementationParameter(Runtime.class, runtime));
        job.recordProducer = ComponentUtils.findImplementationsChain(
                "record producer",
                RecordProducer.class,
                producersPackages,
                parameters);
        job.listeners = ComponentUtils.findImplementations(
                "record producer",
                ProducedRecordListener.class,
                producersPackages,
                parameters,
                null);

        final long totalCount = getLongRequired(cfg, MESSAGES_COUNT);
        final Long warmUpPercent = getLong(cfg, WARM_UP_PERCENT);
        if (warmUpPercent == null) {
            job.warmUpCount = 0l;
            job.testCount = totalCount;
        } else {
            job.warmUpCount = totalCount * warmUpPercent / 100;
            job.testCount = totalCount - job.warmUpCount;
        }

        job.delay = PropertiesUtils.getDuration(cfg, KafkyProducerConfig.DELAY);

        return job;
    }

    protected Properties kafkaProducerProperties;

    protected long warmUpCount;
    protected long testCount;

    protected boolean initialized;
    protected RecordProducer recordProducer;

    protected KafkaProducer<Object, Object> kafkaProducer;

    protected Duration delay;

    protected long messageSendSeq;
    protected long messageLogSeq;

    protected List<ProducedRecordListener> listeners;

    protected LinkedList<ProducerRecord<Object, Object>> warmUpRecors = new LinkedList<>();
    protected LinkedList<ProducerRecord<Object, Object>> testRecors = new LinkedList<>();

    public ProducerJob(final Runtime runtime, final String name) {
        super(runtime, "producer", name);
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

        if (warmUpRecors.size() < warmUpCount) {
            warmUpRecors.add(message);
        } else {
            testRecors.add(message);
        }

        return testRecors.size() < testCount ? go() : stop();
    }

    @Override
    public IterationResult start() throws Exception {
        kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        return stop();
    }

    @Override
    public IterationResult warmUp() throws Exception {
        return sendOneMessage(warmUpRecors);
    }

    @Override
    public IterationResult run() throws Exception {

        if (delay != null) {
            parkNanos(delay.toNanos());
        }

        return sendOneMessage(testRecors);
    }

    protected IterationResult sendOneMessage(LinkedList<ProducerRecord<Object, Object>> recors) {
        final ProducerRecord<Object, Object> rec = recors.poll();
        if (rec == null) {
            return IterationResult.stop();
        } else {
            messageSendSeq++;
            kafkaProducer.send(rec, (Callback) (metadata, exception) -> notifyListeners(messageLogSeq, rec, metadata, exception));
            return produced(1, recors.isEmpty());
        }
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

        appendFieldValue(info, "warm-up-count", warmUpCount);
        appendFieldValue(info, "test-count", testCount);

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
