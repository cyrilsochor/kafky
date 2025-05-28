package io.github.cyrilsochor.kafky.core.global;

import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getBoolean;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getIntegerRequired;
import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.getLongRequired;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.core.config.KafkyExecutorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class KafkyExecutor implements ExecutorService, Component {

    private static final Logger LOG = LoggerFactory.getLogger(KafkyExecutor.class);
    protected static final String LOG_EXECUTE_MESSAGE = "Execution {} {}: {}";

    protected static class LogingRunnable implements Runnable {

        protected final Long seq;// nullable
        protected final Runnable command;

        protected LogingRunnable(Long seq, Runnable command) {
            super();
            this.seq = seq;
            this.command = command;
        }

        @Override
        public void run() {
            LOG.atDebug().setMessage(LOG_EXECUTE_MESSAGE)
                    .addArgument((Supplier<String>) () -> seq == null ? null : format("%6d", seq))
                    .addArgument("start")
                    .addArgument(command)
                    .log();
            command.run();
            LOG.atDebug().setMessage(LOG_EXECUTE_MESSAGE)
                    .addArgument((Supplier<String>) () -> seq == null ? null : format("%6d", seq))
                    .addArgument("finish")
                    .addArgument(command)
                    .log();
        }
    };

    public static KafkyExecutor of(final Map<Object, Object> cfg) {
        return new KafkyExecutor(
                getIntegerRequired(cfg, KafkyExecutorConfig.CORE_POOL_SIZE),
                getIntegerRequired(cfg, KafkyExecutorConfig.MAXIMUM_POOL_SIZE),
                getLongRequired(cfg, KafkyExecutorConfig.KEEP_ALIVE_TIME),
                getIntegerRequired(cfg, KafkyExecutorConfig.WORK_QUEUE_SIZE),
                getBoolean(cfg, KafkyExecutorConfig.ALLOW_CORE_THREAD_TIME_OUT)
        );
    }

    protected final int corePoolSize;
    protected final int maximumPoolSize;
    protected final long keepAliveTime;
    protected final int workQueueSize;
    protected final Boolean allowCoreThreadTimeOut;

    // nullable
    protected ThreadPoolExecutor delegate;

    // nulable
    protected final AtomicLong counter;

    protected KafkyExecutor(final int corePoolSize,
            final int maximumPoolSize,
            final long keepAliveTime,
            final int workQueueSize,
            final Boolean allowCoreThreadTimeOut) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.workQueueSize = workQueueSize;
        this.allowCoreThreadTimeOut = allowCoreThreadTimeOut;
        this.counter = LOG.isDebugEnabled() ? new AtomicLong() : null;
    }

    @Override
    public void init() throws Exception {
        delegate = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(workQueueSize));
        if (allowCoreThreadTimeOut != null) {
            delegate.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
        }
    }

    @Override
    public void close() throws Exception {
        if (delegate != null) {
            delegate.shutdown();
        }
    }

    protected Long nextSequence() {
        return counter != null ? counter.incrementAndGet() : null;
    }

    @Override
    public void execute(final Runnable command) {
        delegate.execute(command);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        return delegate.submit(task);
    }

    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
        return delegate.submit(task, result);
    }

    @Override
    public Future<?> submit(final Runnable task) {
        final Long seq = nextSequence();
        LOG.atDebug().setMessage(LOG_EXECUTE_MESSAGE)
                .addArgument((Supplier<String>) () -> seq == null ? null : format("%6d", seq))
                .addArgument("plan")
                .addArgument(task)
                .log();
        return delegate.submit(new LogingRunnable(seq, task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
            throws InterruptedException {
        return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }

}
