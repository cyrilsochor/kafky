package io.github.cyrilsochor.kafky.core.runtime;

import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_CONSUMER_PROPERTIES;
import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_PRODUCER_PROPERTIES;
import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_REPORT_INTERVAL;
import static io.github.cyrilsochor.kafky.core.runtime.JobState.CANCELING;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static io.github.cyrilsochor.kafky.core.util.ObjectUtils.firstNonNullLong;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.addProperties;

import io.github.cyrilsochor.kafky.core.config.KafkyConfiguration;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.config.ProducerConfig;
import io.github.cyrilsochor.kafky.core.runtime.job.consumer.ConsumerJob;
import io.github.cyrilsochor.kafky.core.runtime.job.producer.ProducerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class Runtime {

    private static final Logger LOG = LoggerFactory.getLogger(Runtime.class);

    public class Shutdown implements Runnable {

        public Shutdown() {
        }

        @Override
        public void run() {
            if (exitStatus == null) {

                int cancelingCount = 0;
                for (final JobThread thread : threads) {
                    if (!thread.jobState.isFinite()) {
                        thread.jobState = CANCELING;
                        cancelingCount++;
                    }
                }
                LOG.info("Shutdown - canceling {} jobs", cancelingCount);

                for (int i = 0; i < 10; i++) {
                    if (exitStatus != null) {
                        return;
                    }

                    LOG.info("Waiting for cancel #{}", i);
                    LockSupport.parkNanos(1_000_000_000l);
                }
                LOG.info("Exit - unable to cancel all runnig jobs");
            }
        }

    }

    protected final List<JobThread> threads = new LinkedList<>();
    protected Integer exitStatus;
    protected Instant start = Instant.now();

    public void run(final KafkyConfiguration cfg) throws Exception {
        validateBasic(cfg);
        createJobs(cfg);
        startJobs();
        waitJobs(cfg);

        exitStatus = cookExitStatus();
        final Duration duration = Duration.between(start, Instant.now());
        LOG.info("Finish status: {}, duration: {}", exitStatus, duration.truncatedTo(ChronoUnit.SECONDS));
        System.exit(exitStatus);
    }


    protected void validateBasic(final KafkyConfiguration cfg) {
        assertTrue(0 < cfg.consumers().size() + cfg.producers().size(), "At least one consumer or producer is required");
    }

    protected synchronized void createJobs(final KafkyConfiguration cfg) throws Exception {
        final List<Job> jobs = new LinkedList<>();

        for (final Map.Entry<Object, Object> consumerEntry : cfg.consumers().entrySet()) {
            final Properties props = new Properties();
            addProperties(props, (Map<Object, Object>) consumerEntry.getValue(), KafkyConsumerConfig.PREFIX);
            addProperties(props, cfg.globalConsumers());
            addProperties(props, cfg.global());
            addProperties(props, DEFAULT_CONSUMER_PROPERTIES);
            jobs.add(ConsumerJob.of((String) consumerEntry.getKey(), props));
        }

        for (final Map.Entry<Object, Object> producerEntry : cfg.producers().entrySet()) {
            final Properties props = new Properties();
            addProperties(props, (Map<Object, Object>) producerEntry.getValue(), KafkyProducerConfig.PREFIX);
            addProperties(props, cfg.globalProducers());
            addProperties(props, cfg.global());
            addProperties(props, DEFAULT_PRODUCER_PROPERTIES);
            jobs.add(ProducerJob.of((String) producerEntry.getKey(), props));
        }

        for (final Job job : jobs) {
            threads.add(JobThread.of(job));
        }
    }

    protected synchronized void startJobs() {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(new Shutdown(), "Shutdown"));
        for (final Thread thread : threads) {
            thread.start();
        }
    }

    protected synchronized void report() {
        final Map<JobState, AtomicLong> threadCountByState = new TreeMap<>();
        long consumedMessagesCount = 0;
        long producedMessagesCount = 0;

        for (final JobThread thread : threads) {
            Function<? super JobState, ? extends AtomicLong> x = s -> new AtomicLong(0);
            threadCountByState.computeIfAbsent(thread.jobState, x).incrementAndGet();
            consumedMessagesCount += thread.jobStatistics.getConsumedMessagesCount();
            producedMessagesCount += thread.jobStatistics.getProducedMessagesCount();
        }

        LOG.info("Report| {}, consumed messages: {}, produced messages: {}",
                threadCountByState.entrySet().stream()
                        .map(e -> String.format("%s jobs: %s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", ")),
                consumedMessagesCount,
                producedMessagesCount);
    }

    protected void waitJobs(final KafkyConfiguration cfg) {
        while (true) {
            long timeout = firstNonNullLong((Number) cfg.report().get(ProducerConfig.SUBSCRIBES_OFFSET), DEFAULT_REPORT_INTERVAL);

            boolean allFinite = true;
            for (final JobThread thread : threads) {
                if (!thread.jobState.isFinite()) {
                    try {
                        LOG.trace("Joining {}", thread.getName());
                        thread.join(timeout);
                        LOG.trace("Joined {}", thread.getName());
                        if (!thread.jobState.isFinite()) {
                            allFinite = false;
                        }
                    } catch (InterruptedException e) {
                        LOG.trace("Interrupted join: {}", thread.getName());
                        timeout = 1;
                        allFinite = false;
                    }
                }
            }

            report();

            if (allFinite) {
                break;
            }
        }
    }

    protected int cookExitStatus() {
        return threads.stream()
                .mapToInt(t -> t.jobState.getExitStatus())
                .max()
                .orElse(2);
    }

}
