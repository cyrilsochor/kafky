package io.github.cyrilsochor.kafky.core.runtime;

import static io.github.cyrilsochor.kafky.api.job.JobState.CANCELING;
import static io.github.cyrilsochor.kafky.api.job.JobState.INITIALIZING;
import static io.github.cyrilsochor.kafky.api.job.JobState.STATE_COMPARATOR;
import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_CONSUMER_CONFIGURATION;
import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_PRODUCER_CONFIGURATION;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.addProperties;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.api.runtime.RuntimeStatus;
import io.github.cyrilsochor.kafky.core.config.KafkyConfiguration;
import io.github.cyrilsochor.kafky.core.config.KafkyDefaults;
import io.github.cyrilsochor.kafky.core.report.Report;
import io.github.cyrilsochor.kafky.core.runtime.job.consumer.ConsumerJob;
import io.github.cyrilsochor.kafky.core.runtime.job.producer.ProducerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("unchecked")
public class Runtime implements RuntimeStatus {

    private static final Logger LOG = LoggerFactory.getLogger(Runtime.class);

    public class ShutdownHook implements Runnable {

        @Override
        public void run() {
            if (exitStatus == null) {

                int cancelingCount = 0;
                for (final JobThread thread : threads) {
                    if (!thread.getJobState().isFinite()) {
                        thread.setJobState(CANCELING);
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

                shutdown();
            }
        }

    }

    protected final List<JobThread> threads = new LinkedList<>();
    protected JobState minJobState = INITIALIZING;
    protected AtomicLong minJobStateCounter = new AtomicLong();
    protected Report report;
    protected Integer exitStatus;
    protected Instant start = Instant.now();
    protected Instant finish;
    protected transient boolean shutdownJobCalled = false;

    public void run(final KafkyConfiguration cfg) throws Exception {
        validateBasic(cfg);
        createReport(cfg);
        createJobs(cfg);
        startJobs();
        waitJobs(cfg);

        shutdown();
        exitStatus = cookExitStatus();
        report.report("FINISHED exit status: %d, duration: %s",
                exitStatus,
                Duration.between(start, finish).truncatedTo(ChronoUnit.SECONDS));
        System.exit(exitStatus);
    }

    protected void validateBasic(final KafkyConfiguration cfg) {
        assertTrue(0 < cfg.consumers().size() + cfg.producers().size(), "At least one consumer or producer is required");
    }

    private void createReport(KafkyConfiguration cfg) {
        final Properties props = new Properties();
        addProperties(props, cfg.report());
        addProperties(props, KafkyDefaults.DEFAULT_REPORT_PROPERTIES);
        report = Report.of(props);
    }

    protected synchronized void createJobs(final KafkyConfiguration cfg) throws Exception {
        final List<Job> jobs = new LinkedList<>();

        for (final Map.Entry<Object, Object> consumerEntry : cfg.consumers().entrySet()) {
            final Map<Object, Object> jobCfg = new HashMap<>((Map<Object, Object>) consumerEntry.getValue());
            addProperties(jobCfg, cfg.globalConsumers());
            addProperties(jobCfg, cfg.global());
            addProperties(jobCfg, DEFAULT_CONSUMER_CONFIGURATION);
            jobs.add(ConsumerJob.of(this, (String) consumerEntry.getKey(), jobCfg));
        }

        for (final Map.Entry<Object, Object> producerEntry : cfg.producers().entrySet()) {
            final Map<Object, Object> jobCfg = new HashMap<>((Map<Object, Object>) producerEntry.getValue());
            addProperties(jobCfg, cfg.globalProducers());
            addProperties(jobCfg, cfg.global());
            addProperties(jobCfg, DEFAULT_PRODUCER_CONFIGURATION);
            jobs.add(ProducerJob.of(this, (String) producerEntry.getKey(), jobCfg));
        }

        for (final Job job : jobs) {
            threads.add(JobThread.of(this, job));
        }
    }

    protected synchronized void startJobs() {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(), "Shutdown"));
        for (final Thread thread : threads) {
            thread.start();
        }
    }

    protected synchronized String jobStatusReport() {
        final Map<JobState, List<String>> threadCountByState = new TreeMap<>();
        long consumedMessagesCount = 0;
        long producedMessagesCount = 0;

        for (final JobThread thread : threads) {
            threadCountByState.computeIfAbsent(thread.getJobState(), s -> new LinkedList<>()).add(thread.getName());
            consumedMessagesCount += thread.jobStatistics.getConsumedMessagesCount();
            producedMessagesCount += thread.jobStatistics.getProducedMessagesCount();
        }

        return format("Produced: %8d, consumed: %8d, jobs: %s",
                producedMessagesCount,
                consumedMessagesCount,
                threadCountByState.entrySet().stream()
                        .map(e -> format("%s %s %s", e.getValue().size(), e.getKey(), e.getValue()))
                        .collect(joining(", ")));
    }

    public Report getReport() {
        return report;
    }

    protected void waitJobs(final KafkyConfiguration cfg) {
        final long jobsStatusPeriod = report.getJobsStatusPeriod();
        while (true) {
            long iterationStart = System.currentTimeMillis();

            boolean allFinite = true;
            for (final JobThread thread : threads) {
                if (!thread.getJobState().isFinite()) {
                    try {
                        LOG.trace("Joining {}", thread.getName());
                        thread.join(Math.max(1, jobsStatusPeriod + iterationStart - System.currentTimeMillis()));
                        LOG.trace("Joined {}", thread.getName());
                        if (!thread.getJobState().isFinite()) {
                            allFinite = false;
                        }
                    } catch (InterruptedException e) {
                        LOG.trace("Interrupted join: {}", thread.getName());
                        allFinite = false;
                    }
                }
            }

            report.report(this::jobStatusReport);

            if (allFinite) {
                break;
            }
        }
    }

    protected void shutdown() {
        if (!shutdownJobCalled) {
            finish = Instant.now();
            shutdownJobCalled = true;
            for (final JobThread thread : threads) {
                thread.shutdownHook();
            }
        }
    }

    protected int cookExitStatus() {
        return threads.stream()
                .mapToInt(t -> t.getJobState().getExitStatus())
                .max()
                .orElse(2);
    }

    public void waitForAllAtLeast(final JobState expectedJobState) {
        while (expectedJobState.ordinal() > minJobState.ordinal()) {
            LOG.debug("Wating for all job at minumum state {}, actual minimal state is {}", expectedJobState, minJobState);
            synchronized (minJobStateCounter) {
                try {
                    minJobStateCounter.wait(1000);
                } catch (InterruptedException e) {
                    LOG.info("Interupted", e);
                }
            }
        }
    }

    public void stateChanged() {
        synchronized (minJobStateCounter) {
            final JobState newMinJobState = getMinState(threads);
            if (minJobState != newMinJobState) {
                minJobState = newMinJobState;
                minJobStateCounter.incrementAndGet();
                minJobStateCounter.notifyAll();
            }
        }
    }

    protected JobState getMinState(final Collection<JobThread> t) {
        return t.stream()
                .map(JobThread::getJobState)
                .min(STATE_COMPARATOR)
                .orElse(INITIALIZING);
    }

    protected JobState getMaxState(final Collection<JobThread> t) {
        return t.stream()
                .map(JobThread::getJobState)
                .max(STATE_COMPARATOR)
                .orElse(JobState.SUCCESS);
    }

    protected Collection<JobThread> getProducerThreads() {
        return threads.stream()
                .filter(t -> t.job instanceof ProducerJob)
                .toList();
    }

    @Override
    public Instant getStart() {
        return start;
    }

    @Override
    public Instant getFinish() {
        return finish;
    }

    @Override
    public JobState getMinJobState() {
        return getMinState(threads);
    }

    @Override
    public JobState getMaxJobState() {
        return getMaxState(threads);
    }

    @Override
    public JobState getMinProducerState() {
        return getMinState(getProducerThreads());
    }

}
