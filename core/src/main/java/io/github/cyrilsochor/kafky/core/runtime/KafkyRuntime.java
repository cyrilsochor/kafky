package io.github.cyrilsochor.kafky.core.runtime;

import static io.github.cyrilsochor.kafky.api.job.JobState.CANCELING;
import static io.github.cyrilsochor.kafky.api.job.JobState.INITIALIZING;
import static io.github.cyrilsochor.kafky.api.job.JobState.STATE_COMPARATOR;
import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_CONSUMER_CONFIGURATION;
import static io.github.cyrilsochor.kafky.core.config.KafkyDefaults.DEFAULT_PRODUCER_CONFIGURATION;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertNotNull;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertTrue;
import static io.github.cyrilsochor.kafky.core.util.ComponentUtils.createImplementation;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.addProperties;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getClassRequired;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.api.runtime.RuntimeStatus;
import io.github.cyrilsochor.kafky.core.config.KafkyComponentConfig;
import io.github.cyrilsochor.kafky.core.config.KafkyConfiguration;
import io.github.cyrilsochor.kafky.core.config.KafkyDefaults;
import io.github.cyrilsochor.kafky.core.report.Report;
import io.github.cyrilsochor.kafky.core.runtime.job.consumer.ConsumerJob;
import io.github.cyrilsochor.kafky.core.runtime.job.producer.ProducerJob;
import io.github.cyrilsochor.kafky.core.util.ComponentUtils.ImplementationParameter;
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
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class KafkyRuntime implements RuntimeStatus {

    private static final Logger LOG = LoggerFactory.getLogger(KafkyRuntime.class);
    private static final String LOG_PROPERTIES_MSG = "{} {} properties:\n{}";

    protected class ShutdownHook implements Runnable {

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

    protected static final class ConfigLogSupplier implements Supplier<String> {
        private final Map<Object, Object> jobCfg;

        private ConfigLogSupplier(Map<Object, Object> jobCfg) {
            this.jobCfg = jobCfg;
        }

        @Override
        public String get() {
            return jobCfg.entrySet().stream()
                    .map(e -> format("%s: %s", e.getKey(), e.getValue()))
                    .collect(joining("\n"));
        }
    }

    protected final List<JobThread> threads = new LinkedList<>();
    protected final Map<String, Component> globalComponents = new HashMap<>();
    protected JobState minJobState = INITIALIZING;
    protected AtomicLong minJobStateCounter = new AtomicLong();
    protected ConcurrentLinkedDeque<OverallStateChangedListener> overallStateChangedListeners = new ConcurrentLinkedDeque<>();
    protected Report report;
    protected Integer exitStatus;
    protected Instant start = Instant.now();
    protected Instant finish;
    protected transient boolean shutdownJobCalled = false;

    public void run(final KafkyConfiguration cfg) throws Exception {
        validateBasic(cfg);
        createReport(cfg);
        createGlobalComponents(cfg);
        try {
            createJobs(cfg);
            initGlobalComponents();
            startJobs();
            waitJobs(cfg);
        } finally {
            closeGlobalComponents();
        }

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

    protected void createReport(KafkyConfiguration cfg) {
        final Properties props = new Properties();
        addProperties(props, cfg.report());
        addProperties(props, KafkyDefaults.DEFAULT_REPORT_PROPERTIES);
        report = Report.of(props);
    }

    protected synchronized void createGlobalComponents(final KafkyConfiguration cfg) throws Exception {
        for (final Map.Entry<Object, Object> componentEntry : cfg.components().entrySet()) {
            final String componentId = (String) componentEntry.getKey();
            final Map<Object, Object> componentCfg = new HashMap<>((Map<Object, Object>) componentEntry.getValue());
            addProperties(componentCfg, cfg.global());
            LOG.atDebug().setMessage(LOG_PROPERTIES_MSG)
                    .addArgument("Global component")
                    .addArgument(componentId)
                    .addArgument(new ConfigLogSupplier(componentCfg))
                    .log();
            final Class<? extends Component> componentClass = getClassRequired(componentCfg, Component.class, KafkyComponentConfig.CLASS);
            final Component component = createImplementation(
                    "global",
                    componentClass,
                    List.of(
                            new ImplementationParameter(Map.class, componentCfg),
                            new ImplementationParameter(KafkyRuntime.class, this)));
            if (component != null) {
                report.report("Component %s CREATED: %s", componentId, component.getComponentInfo());
                globalComponents.put(componentId, component);
            }
        }
    }

    protected synchronized void createJobs(final KafkyConfiguration cfg) throws Exception {
        final List<Job> jobs = new LinkedList<>();

        for (final Map.Entry<Object, Object> consumerEntry : cfg.consumers().entrySet()) {
            final String name = (String) consumerEntry.getKey();
            final Map<Object, Object> jobCfg = new HashMap<>((Map<Object, Object>) consumerEntry.getValue());
            addProperties(jobCfg, cfg.globalConsumers());
            addProperties(jobCfg, cfg.global());
            addProperties(jobCfg, DEFAULT_CONSUMER_CONFIGURATION);
            LOG.atDebug().setMessage(LOG_PROPERTIES_MSG)
                    .addArgument("Consumer job")
                    .addArgument(name)
                    .addArgument(new ConfigLogSupplier(jobCfg))
                    .log();
            final ConsumerJob job = ConsumerJob.of(this, name, jobCfg);
            report.report("Job %s CREATED: %s", job.getId(), job.getInfo());
            jobs.add(job);
        }

        for (final Map.Entry<Object, Object> producerEntry : cfg.producers().entrySet()) {
            final String name = (String) producerEntry.getKey();
            final Map<Object, Object> jobCfg = new HashMap<>((Map<Object, Object>) producerEntry.getValue());
            addProperties(jobCfg, cfg.globalProducers());
            addProperties(jobCfg, cfg.global());
            addProperties(jobCfg, DEFAULT_PRODUCER_CONFIGURATION);
            LOG.atDebug().setMessage(LOG_PROPERTIES_MSG)
                    .addArgument("Producer job")
                    .addArgument(name)
                    .addArgument(new ConfigLogSupplier(jobCfg))
                    .log();
            final ProducerJob job = ProducerJob.of(this, name, jobCfg);
            report.report("Job %s CREATED: %s", job.getId(), job.getInfo());
            jobs.add(job);
        }

        for (final Job job : jobs) {
            threads.add(JobThread.of(this, job));
        }
    }

    protected void initGlobalComponents() throws Exception {
        for (final Component component : globalComponents.values()) {
            component.init();
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
        long asyncJobsCount = 0;

        for (final JobThread thread : threads) {
            threadCountByState.computeIfAbsent(thread.getJobState(), s -> new LinkedList<>()).add(thread.getName());
            consumedMessagesCount += thread.jobStatistics.getConsumedMessagesCount();
            producedMessagesCount += thread.jobStatistics.getProducedMessagesCount();
            asyncJobsCount += thread.job.getAsyncTasksCount();
        }

        return format("Produced: %8d, consumed: %8d, %sjobs: %s",
                producedMessagesCount,
                consumedMessagesCount,
                asyncJobsCount == 0 ? "" : format("async tasks: %8d, ", asyncJobsCount),
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

    protected void closeGlobalComponents() throws Exception {
        for (final Component component : globalComponents.values()) {
            component.close();
        }
    }

    protected void shutdown() {
        if (!shutdownJobCalled) {
            finish = Instant.now();
            shutdownJobCalled = true;
            for (final JobThread thread : threads) {
                thread.shutdownHook();
            }
            for (final Component component : globalComponents.values()) {
                component.shutdownHook();
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
                fireOverallStateChanged(newMinJobState);
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
    public String getUser() {
        final String rawUsername = System.getProperty("user.name");
        return rawUsername == null ? null : rawUsername.toLowerCase();
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

    public <I> I getGlobalComponent(final String componentId, final Class<? extends I> componentInterface) {
        final Component component = globalComponents.get(componentId);
        assertNotNull(component, () -> format("Global component '%s' not found", componentId));
        assertTrue(componentInterface.isAssignableFrom(component.getClass()),
                () -> format("Unexpected global component '%s' class, expected  %s, actual %s",
                        componentId,
                        componentInterface.getName(),
                        component.getClass().getName()));
        return (I) component;
    }

    public <I> Collection<I> getGlobalComponentsByType(final Class<? extends I> componentInterface) {
        return globalComponents.values().stream()
                .map(gc -> {
                    if (componentInterface.isAssignableFrom(gc.getClass())) {
                        return (I) gc;
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }

    public void addOverallStateChangedListener(final OverallStateChangedListener listener) {
        overallStateChangedListeners.add(listener);
    }

    public boolean removeOverallStateChangedListener(final OverallStateChangedListener listener) {
        return overallStateChangedListeners.remove(listener);
    }

    protected void fireOverallStateChanged(final JobState newState) {
        LOG.debug("Fire overall state changed to {} start", newState);
        for (final OverallStateChangedListener listener : overallStateChangedListeners) {
            listener.overallStateChanged(newState);
        }
        LOG.debug("Fire overall state changed to {} finish, newState");
    }

}
