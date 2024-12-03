package io.github.cyrilsochor.kafky.core.runtime.job;

import static io.github.cyrilsochor.kafky.api.job.JobState.INITIALIZING;

import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.core.runtime.Job;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJob.class);

    protected final Runtime runtime;
    protected final String kind;
    protected final String name;
    protected final String id;
    protected JobState state = INITIALIZING;

    public AbstractJob(final Runtime runtime, final String kind, final String name) {
        this.runtime = runtime;
        this.kind = kind;
        this.name = name;
        this.id = kind + "-" + name;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public JobState getState() {
        return state;
    }

    @Override
    public void setState(final JobState jobState) {
        LOG.debug("Job {} {}", getId(), state);
        this.state = jobState;
        runtime.stateChanged();
    }

}
