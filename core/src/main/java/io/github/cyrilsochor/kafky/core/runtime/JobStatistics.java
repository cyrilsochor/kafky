package io.github.cyrilsochor.kafky.core.runtime;

import java.time.Duration;
import java.time.Instant;

public class JobStatistics {

    private Instant start;
    private Instant finish;
    private long consumedMessagesCount;
    private long producedMessagesCount;

    public JobStatistics() {
    }

    public void incrementConsumedRecordsCount(final long consumendMessagesCount) {
        this.consumedMessagesCount += consumendMessagesCount;
    }

    public void incrementProducesRecordsCount(long producedMessagesCount) {
        this.producedMessagesCount += producedMessagesCount;
    }

    public long getConsumedMessagesCount() {
        return consumedMessagesCount;
    }

    public long getProducedMessagesCount() {
        return producedMessagesCount;
    }

    public void start() {
        this.start = Instant.now();
    }

    public void finish() {
        this.finish = Instant.now();
    }

    public Duration getDuration() {
        if (start == null || finish == null) {
            return null;
        } else {
            return Duration.between(finish, start);
        }
    }

}
