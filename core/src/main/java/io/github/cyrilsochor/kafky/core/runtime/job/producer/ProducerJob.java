package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.MESSAGES_COUNT;
import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.WARM_UP_PERCENT;
import static io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig.MEASURE_RESPONSE_TIME_PERCENT;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.go;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.produced;
import static io.github.cyrilsochor.kafky.core.runtime.IterationResult.stop;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldKey;
import static io.github.cyrilsochor.kafky.core.util.InfoUtils.appendFieldValue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getLongRequired;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import io.github.cyrilsochor.kafky.api.job.FutureRegistry;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecordListener;
import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.runtime.IterationResult;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.runtime.KafkyRuntime;
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
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

public class ProducerJob extends AbstractJob implements Job, FutureRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerJob.class);

    public static ProducerJob of(
            final KafkyRuntime runtime,
            final String name,
            final Map<Object, Object> cfg) throws IOException {
        final ProducerJob job = new ProducerJob(runtime, name);

        job.kafkaProducerProperties = new Properties();
        job.kafkaProducerProperties.putAll(PropertiesUtils.getMapRequired(cfg, KafkyProducerConfig.PROPERITES));

        final List<String> producersPackages = PropertiesUtils.getNonEmptyListOfStrings(cfg, KafkyProducerConfig.RECORD_PRODUCERS_PACKAGES);
        final List<ImplementationParameter> parameters = List.of(
                new ImplementationParameter(Map.class, cfg),
                new ImplementationParameter(KafkyRuntime.class, runtime),
                new ImplementationParameter(FutureRegistry.class, job));
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
        final long warmUpPercent = getLongRequired(cfg, WARM_UP_PERCENT);
        final long measureResponseTimePercent = getLongRequired(cfg, MEASURE_RESPONSE_TIME_PERCENT);
        job.warmUpCount = totalCount * warmUpPercent / 100;
        job.measureResponseTimeCount = totalCount * measureResponseTimePercent / 100;
        job.measureThroughputCount = totalCount - job.warmUpCount - job.measureResponseTimeCount;

        job.warmUpDelay = PropertiesUtils.getDuration(cfg, KafkyProducerConfig.WARM_UP_DELAY);
        job.measureResponseTimeDelay = PropertiesUtils.getDuration(cfg, KafkyProducerConfig.MEASURE_RESPONSE_TIME_DELAY);
        job.measureThroughputDelay = PropertiesUtils.getDuration(cfg, KafkyProducerConfig.MEASURE_THROUGHPUT_DELAY);

        return job;
    }

    protected Properties kafkaProducerProperties;

    protected long warmUpCount;
    protected long measureResponseTimeCount;
    protected long measureThroughputCount;

    protected boolean initialized;
    protected RecordProducer recordProducer;

    protected KafkaProducer<Object, Object> kafkaProducer;

    protected Duration warmUpDelay;
    protected Duration measureResponseTimeDelay;
    protected Duration measureThroughputDelay;

    protected long messageSendSeq;
    protected long messageLogSeq;

    protected List<ProducedRecordListener> listeners;

    protected LinkedList<ProducerRecord<Object, Object>> warmUpRecors = new LinkedList<>();
    protected LinkedList<ProducerRecord<Object, Object>> measureResponseTimeRecors = new LinkedList<>();
    protected LinkedList<ProducerRecord<Object, Object>> measureThroughputRecors = new LinkedList<>();

    protected final Deque<Future<?>> futures = new ConcurrentLinkedDeque<>();

    public ProducerJob(final KafkyRuntime runtime, final String name) {
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
        } else if (measureResponseTimeRecors.size() < measureResponseTimeCount) {
            measureResponseTimeRecors.add(message);
        } else {
            measureThroughputRecors.add(message);
        }

        if (measureThroughputRecors.size() < measureThroughputCount) {
            return go();
        } else {
            finishAllFutures();
            return stop();
        }
    }

    @Override
    public IterationResult start() throws Exception {
        kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        return stop();
    }

    @Override
    public IterationResult warmUp() throws Exception {
        delay(warmUpDelay);
        return sendOneMessage(warmUpRecors);
    }

    @Override
    public IterationResult measureResponseTime() throws Exception {
        delay(measureResponseTimeDelay);
        return sendOneMessage(measureResponseTimeRecors);
    }

    @Override
    public IterationResult measureThroughput() throws Exception {
        delay(measureThroughputDelay);
        return sendOneMessage(measureThroughputRecors);
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
        appendFieldValue(info, "measure-response-time-count", measureResponseTimeCount);
        appendFieldValue(info, "measure-throughput-count", measureThroughputCount);

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

    @Override
    public void addFuture(Future<?> future) {
        LOG.debug("Adding future {}, current {} futures", future, this.futures.size());
        this.futures.add(future);
    }

    protected void finishAllFutures() throws InterruptedException, ExecutionException {
        LOG.debug("Finishing {} futures", futures.size());
        while (!futures.isEmpty()) {
            final Future<?> future = futures.getFirst();
            final Object result = future.get();
            LOG.debug("Future finished with result {}", result);
            futures.remove(future);
        }
    }

    @Override
    public long getAsyncTasksCount() {
        return futures.size();
    }

    protected void delay(/*nullable*/ final Duration duration) {
        if (duration != null) {
            parkNanos(duration.toNanos());
        }
    }

}
