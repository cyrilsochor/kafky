package io.github.cyrilsochor.kafky.api.runtime;

import io.github.cyrilsochor.kafky.api.job.JobState;

import java.time.Instant;

public interface RuntimeStatus {

    JobState getMinJobState();

    JobState getMaxJobState();

    JobState getMinProducerState();

    Instant getStart();

    Instant getFinish();

    // nullable
    String getUser();

}
