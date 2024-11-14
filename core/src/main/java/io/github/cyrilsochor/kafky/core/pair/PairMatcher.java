package io.github.cyrilsochor.kafky.core.pair;

import static java.lang.Math.max;
import static java.lang.Math.min;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

public class PairMatcher {

    private static final Logger LOG = LoggerFactory.getLogger(PairMatcher.class);

    protected static class Pair {
        protected String key;
        protected boolean warmUp;
        protected long pairTimestamp;
        protected long durationMillis;
        protected ProducedRecord request;
        protected ConsumerRecord<Object, Object> response;

        @Override
        public String toString() {
            return "Pair [key=" + key + ", warmUp=" + warmUp + ", pairTimestamp=" + pairTimestamp + ", durationMillis=" + durationMillis + "]";
        }

    }

    protected static final Map<String, Pair> openPairs = new HashMap<>();
    protected static final Collection<Pair> finishedPairs = new ArrayList<>();
    protected static long totalCount = 0;
    protected static long passersByCount = 0;

    protected static long firstRequestTimestamp = Long.MAX_VALUE;
    protected static long lastRequestTimestamp = Long.MIN_VALUE;
    protected static long firstResponseTimestamp = Long.MAX_VALUE;
    protected static long lastResponseTimestamp = Long.MIN_VALUE;
    protected static List<Long> responseDurationsMs = new ArrayList<>();
    protected static Map<Integer, Instant> requestProducers = new ConcurrentHashMap<>();

    public static void addRequest(final String key, final ProducedRecord record) {
        synchronized (openPairs) {
            totalCount++;

            final Pair pair = new Pair();
            pair.key = key;
            pair.request = record;
            openPairs.put(key, pair);
        }
    }

    public static void addResponse(final String key, final ConsumerRecord<Object, Object> record, final boolean warmup) {
        synchronized (openPairs) {
            final Pair pair = openPairs.remove(key);
            if (pair == null) {
                LOG.debug("Unmatched response {}: {}", key, record);
                passersByCount++;
                return;
            }

            pair.warmUp = warmup;
            pair.pairTimestamp = System.currentTimeMillis(); // don't use record.timestam() - it's producer timestamp with different clock
            pair.durationMillis = pair.pairTimestamp - pair.request.metadata().timestamp();
            finishedPairs.add(pair);

            if (!pair.warmUp) {
                final long requestTimestamp = pair.request.metadata().timestamp();
                firstRequestTimestamp = min(firstRequestTimestamp, requestTimestamp);
                lastRequestTimestamp = max(lastRequestTimestamp, requestTimestamp);
                firstResponseTimestamp = min(firstResponseTimestamp, pair.pairTimestamp);
                lastResponseTimestamp = max(lastResponseTimestamp, pair.pairTimestamp);
            }

            LOG.debug("Matched pair: {}, firstRequestTimestamp: {}, lastRequestTimestamp: {}, firstResponseTimestamp: {}, lastResponseTimestamp: {}",
                    pair,
                    firstRequestTimestamp, lastRequestTimestamp,
                    firstResponseTimestamp, lastResponseTimestamp);
        }
    }

    public static boolean isAllPaired() {
        return openPairs.isEmpty();
    }

    public static LocalDateTime getTestStart() {
        return firstRequestTimestamp == Long.MAX_VALUE ? null
                : Instant.ofEpochMilli(firstRequestTimestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
    }

    public static LocalDateTime getTestFinish() {
        return lastResponseTimestamp == Long.MIN_VALUE ? null
                : Instant.ofEpochMilli(lastResponseTimestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
    }

    public static long getTestCount() {
        return totalCount;
    }

    public static long getPassersByCount() {
        return passersByCount;
    }

    public static Duration getTestDuration() {
        return lastResponseTimestamp == Long.MIN_VALUE || firstRequestTimestamp == Long.MAX_VALUE ? null
                : Duration.ofMillis(lastResponseTimestamp - firstRequestTimestamp);
    }

    public static Long getThroughputPerMinute() {
        final Duration duration = getTestDuration();
        return duration == null || duration.isZero() ? null
                : totalCount * 1000 * 60 / duration.toMillis();
    }

    // nulable
    public static Duration getTotalDivCountDuration() {
        final Duration duration = getTestDuration();
        return duration == null ? null : duration.dividedBy(totalCount);
    }

    // nulable
    public static Duration getResponseMinDuration() {
        final OptionalLong millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .min();
        return millis.isPresent() ? Duration.ofMillis(millis.getAsLong()) : null;
    }

    // nulable
    public static Duration getResponseAvgDuration() {
        final OptionalDouble millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .average();
        return millis.isPresent() ? Duration.ofMillis((long) millis.getAsDouble()) : null;
    }

    // nulable
    public static Duration getResponseMedDuration() {
        final OptionalDouble millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .sorted()
                .skip((finishedPairs.size() - 1) / 2).limit(2 - finishedPairs.size() % 2)
                .average();
        return millis.isPresent() ? Duration.ofMillis((long) millis.getAsDouble()) : null;
    }

    // nulable
    public static Duration getResponseMaxDuration() {
        final OptionalLong millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .max();
        return millis.isPresent() ? Duration.ofMillis(millis.getAsLong()) : null;
    }

}
