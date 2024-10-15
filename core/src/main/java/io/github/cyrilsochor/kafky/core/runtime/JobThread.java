package io.github.cyrilsochor.kafky.core.runtime;

import static io.github.cyrilsochor.kafky.core.runtime.JobState.*;
import static io.github.cyrilsochor.kafky.core.runtime.JobState.CANCELING;
import static io.github.cyrilsochor.kafky.core.runtime.JobState.FAILED;
import static io.github.cyrilsochor.kafky.core.runtime.JobState.INITIALIZING;
import static io.github.cyrilsochor.kafky.core.runtime.JobState.RUNNING;
import static io.github.cyrilsochor.kafky.core.runtime.JobState.SUCCESS;
import static java.lang.String.format;

import io.github.cyrilsochor.kafky.core.report.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(JobThread.class);

    private static final String JOB_THREAD_NAME = "job%s";

    protected final Job job;
    protected final Report report;
    protected JobState jobState = INITIALIZING;
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
            thread.report.report("Job %s CREATED: %s", thread.job.getId(), thread.job.getInfo());
            thread.jobStatistics.start();
            try {

                iterate("prepare", j -> j.prepare(), PREPARING, PREPARED);

                iterate("start", j -> j.start(), STARTING, STARTED);

                iterate("run", j -> j.run(), RUNNING, SUCCESS);

            } catch (Exception e) {
                thread.jobState = FAILED;
                thread.report.reportException(e, "Job %s FAILED", thread.job.getId());
            } finally {
                try {
                    LOG.debug("Running job {} phase finish", thread.job.getId());
                    thread.job.finish();
                } catch (Exception e) {
                    thread.jobState = FAILED;
                    thread.report.reportException(e, "Job %s finish FAILED", thread.job.getId());
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

            thread.jobState = startState;

            int iterationSeq = 0;
            boolean last = false;
            while (!last) {
                if (thread.jobState == CANCELING) {
                    thread.jobState = CANCELED;
                    thread.report.report("Job %s CANCELED", thread.job.getId());
                    last = true;
                } else {
                    LOG.debug("Execution job {} {} iteration #{}", thread.job.getId(), phase, iterationSeq);
                    final IterationResult result = iteration.execute(thread.job);
                    last = result.last();
                    thread.jobStatistics.incrementConsumedRecordsCount(result.consumendMessagesCount());
                    thread.jobStatistics.incrementProducesRecordsCount(result.producedMessagesCount());
                    if (last) {
                        thread.jobState = finalState;
                        thread.report.report("Job %s %s", thread.job.getId(), finalState);
                    }
                }
                iterationSeq++;
            }
        }

    }

    protected JobThread(
            Runnable runnable,
            Job job,
            String name,
            Report report) {
        super(runnable, name);
        this.job = job;
        this.report = report;
    }

    public static JobThread of(final Job job, Report report) {
        final JobRunnable runnable = new JobRunnable();
        final JobThread thread = new JobThread(runnable, job, format(JOB_THREAD_NAME, job.getId()), report);
        runnable.setThread(thread);
        return thread;
    }



}
