package io.github.cyrilsochor.kafky.core.runtime;

import static io.github.cyrilsochor.kafky.api.job.JobState.*;
import static java.lang.String.format;

import io.github.cyrilsochor.kafky.api.job.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(JobThread.class);

    private static final String JOB_THREAD_NAME = "job%s";

    public static JobThread of(final Runtime runtime, final Job job) {
        final JobRunnable runnable = new JobRunnable();
        final JobThread thread = new JobThread(runtime, runnable, job, format(JOB_THREAD_NAME, job.getId()));
        runnable.setThread(thread);
        return thread;
    }

    protected final Runtime runtime;
    protected final Job job;
    private JobState jobState = INITIALIZING;
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
            thread.runtime.getReport().report("Job %s CREATED: %s", thread.job.getId(), thread.job.getInfo());
            thread.jobStatistics.start();
            try {

                iterate("prepare", j -> j.prepare(), PREPARING, PREPARED);

                thread.runtime.waitForAllAtLeast(PREPARED);
                
                iterate("start", j -> j.start(), STARTING, STARTED);

                thread.runtime.waitForAllAtLeast(STARTED);

                iterate("warmup", j -> j.warmUp(), WARMUP, WARMED);

                thread.runtime.waitForAllAtLeast(WARMED);

                iterate("run", j -> j.run(), RUNNING, SUCCESS);

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
                final JobState finalState) throws Exception {
            LOG.debug("Running job {} phase {}", thread.job.getId(), phase);

            thread.setJobState(startState);

            int iterationSeq = 0;
            boolean last = false;
            while (!last) {
                if (thread.isJobState(CANCELING)) {
                    thread.setJobState(CANCELED);
                    last = true;
                } else {
                    LOG.debug("Execution job {} {} iteration #{}", thread.job.getId(), phase, iterationSeq);
                    final IterationResult result = iteration.execute(thread.job);
                    LOG.debug("Finished  job {} {} iteration #{}, result: {}", thread.job.getId(), phase, iterationSeq, result);
                    last = result.last();
                    thread.jobStatistics.incrementConsumedRecordsCount(result.consumendMessagesCount());
                    thread.jobStatistics.incrementProducesRecordsCount(result.producedMessagesCount());
                    if (last) {
                        thread.setJobState(finalState);
                    }
                }
                iterationSeq++;
            }
        }
    }

    protected JobThread(
            final Runtime runtime,
            final Runnable runnable,
            final Job job,
            final String name) {
        super(runnable, name);
        this.job = job;
        this.runtime = runtime;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public void setJobState(final JobState state) {
        LOG.debug("Job {} {}", job.getId(), state);
        this.jobState = state;
        runtime.stateChanged();
    }

    public boolean isJobState(final JobState expectedJobState) {
        return jobState == expectedJobState;
    }

    public boolean isJobStateAtLeast(final JobState atLeastJobState) {
        return jobState.ordinal() >= atLeastJobState.ordinal();
    }

    public void shutdownHook() {
        job.shutdownHook();
    }

}
