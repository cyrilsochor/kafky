package io.github.cyrilsochor.kafky.core.runtime;

import static io.github.cyrilsochor.kafky.api.job.JobState.*;
import static java.lang.String.format;

import io.github.cyrilsochor.kafky.api.job.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(JobThread.class);

    private static final String JOB_THREAD_NAME = "job%s";

    public static JobThread of(final KafkyRuntime runtime, final Job job) {
        final JobRunnable runnable = new JobRunnable();
        final JobThread thread = new JobThread(runtime, runnable, job, format(JOB_THREAD_NAME, job.getId()));
        runnable.setThread(thread);
        return thread;
    }

    protected final KafkyRuntime runtime;
    protected final Job job;
    protected JobStatistics jobStatistics = new JobStatistics();

    protected interface Iteration {
        IterationResult execute(Job job) throws Exception;
    }

    protected static class JobRunnable implements Runnable {

        private JobThread thread;

        protected void setThread(JobThread thread) {
            this.thread = thread;
        }

        @Override
        public void run() {
            thread.jobStatistics.start();
            try {

                iterate("prepare", j -> j.prepare(), PREPARING, PREPARED);

                thread.runtime.waitForAllAtLeast(PREPARED);

                iterate("start", j -> j.start(), STARTING, STARTED);

                thread.runtime.waitForAllAtLeast(STARTED);

                if (thread.job.isObserve()) {

                    iterate("observe", j -> j.observe(), OBSERVING, SUCCESS);

                } else {

                    iterate("warmup", j -> j.warmUp(), WARMUP, WARMED);

                    thread.runtime.waitForAllAtLeast(WARMED);

                    iterate("measure-response-time", j -> j.measureResponseTime(), MEASURING_RESPONSE_TIME, MEASURED_RESPONSE_TIME);

                    thread.runtime.waitForAllAtLeast(MEASURED_RESPONSE_TIME);

                    iterate("measure-throughput", j -> j.measureThroughput(), MEASURING_THROUGHPUT, SUCCESS);

                }
            } catch (Exception e) {
                thread.setJobState(FAILED);
                thread.runtime.getReport().reportException(e, "Job %s FAILED", thread.job.getId());
            } finally {
                try {
                    LOG.debug("Running job {} phase finish", thread.job.getId());
                    thread.job.finish();
                } catch (Exception e) {
                    thread.setJobState(FAILED);
                    thread.runtime.getReport().reportException(e, "Job %s finish FAILED", thread.job.getId());
                }
                thread.jobStatistics.finish();
            }
        }

        protected void iterate(
                final String phase,
                final Iteration iteration,
                final JobState startState,
                final JobState finishState) throws Exception {
            final JobState actualState = thread.setJobState(startState);
            if (actualState != startState) {
                thread.setJobState(CANCELED);
                LOG.debug("Skipping job {} phase {} because state is {}", thread.job.getId(), phase, actualState);
                return;
            }

            LOG.debug("Running job {} phase {}", thread.job.getId(), phase);

            int iterationSeq = 0;
            boolean last = false;
            while (!last) {
                if (thread.getJobState() == CANCELING) {
                    thread.setJobState(CANCELED);
                    LOG.debug("Canceled job {} in {} iteration #{}", thread.job.getId(), phase, iterationSeq);
                    last = true;
                } else {
                    LOG.debug("Execution job {} {} iteration #{}", thread.job.getId(), phase, iterationSeq);
                    final IterationResult result = iteration.execute(thread.job);
                    LOG.debug("Finished  job {} {} iteration #{}, result: {}", thread.job.getId(), phase, iterationSeq, result);
                    last = result.last();
                    thread.jobStatistics.incrementConsumedRecordsCount(result.consumendMessagesCount());
                    thread.jobStatistics.incrementProducesRecordsCount(result.producedMessagesCount());
                    if (last) {
                        thread.setJobState(finishState);
                    }
                }
                iterationSeq++;
            }
        }
    }

    protected JobThread(
            final KafkyRuntime runtime,
            final Runnable runnable,
            final Job job,
            final String name) {
        super(runnable, name);
        this.job = job;
        this.runtime = runtime;
    }

    public JobState getJobState() {
        return job.getState();
    }

    public JobState setJobState(final JobState state) {
        if (job.getState().compareTo(state) < 0) { // don't allow state decrease, e.g: CANCELING -> WARMUP
            job.setState(state);
        }
        return job.getState();
    }

    public void shutdownHook() {
        job.shutdownHook();
    }

}
