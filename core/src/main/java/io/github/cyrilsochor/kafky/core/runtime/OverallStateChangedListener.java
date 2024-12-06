package io.github.cyrilsochor.kafky.core.runtime;

import io.github.cyrilsochor.kafky.api.job.JobState;

public interface OverallStateChangedListener {

    void overallStateChanged(JobState newState);

}
