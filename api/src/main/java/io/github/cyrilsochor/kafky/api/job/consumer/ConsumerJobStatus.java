package io.github.cyrilsochor.kafky.api.job.consumer;

import io.github.cyrilsochor.kafky.api.job.JobState;

import java.util.Set;

public interface ConsumerJobStatus {

    JobState getState();

    Set<String> getConsumedTopics();

    long getConsumedMessagesCount();

}
