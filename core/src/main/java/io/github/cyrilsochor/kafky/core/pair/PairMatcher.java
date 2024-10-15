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

    protected record Pair(
            long pairTimestamp,
            long durationMillis,
            ProducedRecord producedRecord,
            ConsumerRecord<Object, Object> consumerRecord) {
    }

    protected static final Map<String, ProducedRecord> requests = new HashMap<>();
    protected static final Collection<Pair> pairs = new ArrayList<>();
    protected static long totalCount = 0;
    protected static long passersByCount = 0;

    protected static long firstRequestTimestamp = Long.MAX_VALUE;
    protected static long lastRequestTimestamp = Long.MIN_VALUE;
    protected static long firstResponseTimestamp = Long.MAX_VALUE;
    protected static long lastResponseTimestamp = Long.MIN_VALUE;
    protected static List<Long> responseDurationsMs = new ArrayList<>();
    protected static Map<Integer, Instant> requestProducers = new ConcurrentHashMap<>();

    public static void addRequest(final String key, final ProducedRecord record) {
        long timestamp = record.metadata().timestamp();
        synchronized (requests) {
            totalCount++;
            requests.put(key, record);
            firstRequestTimestamp = min(firstRequestTimestamp, timestamp);
            lastRequestTimestamp = max(lastRequestTimestamp, timestamp);
        }
    }

    public static void addResponse(final String key, final ConsumerRecord<Object, Object> record) {
        synchronized (requests) {
            final ProducedRecord request = requests.remove(key);
            if (request == null) {
                LOG.debug("Unmatched response {}: {}", key, record);
                passersByCount++;
            } else {
                final long timestamp = System.currentTimeMillis(); // don't use record.timestam() - it's producer timestamp with different clock
                final long duration = timestamp - request.metadata().timestamp();
                firstResponseTimestamp = min(firstResponseTimestamp, timestamp);
                lastResponseTimestamp = max(lastResponseTimestamp, timestamp);
                pairs.add(new Pair(timestamp, duration, request, record));
            }
        }
    }

    public static boolean isAllPaired() {
        return requests.isEmpty();
    }

    public static LocalDateTime getStart() {
        return firstRequestTimestamp == Long.MAX_VALUE ? null
                : Instant.ofEpochMilli(firstRequestTimestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
    }

    public static LocalDateTime getFinish() {
        return lastResponseTimestamp == Long.MIN_VALUE ? null
                : Instant.ofEpochMilli(lastResponseTimestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
    }

    public static long getTotalCount() {
        return totalCount;
    }

    public static long getPassersByCount() {
        return passersByCount;
    }

    public static Duration getDuration() {
        return lastResponseTimestamp == Long.MIN_VALUE || firstRequestTimestamp == Long.MAX_VALUE ? null
                : Duration.ofMillis(lastResponseTimestamp - firstRequestTimestamp);
    }

    public static Long getThroughputPerMinute() {
        final Duration duration = getDuration();
        return duration == null || duration.isZero() ? null
                : totalCount * 1000 * 60 / duration.toMillis();
    }

    // nulable
    public static Duration getTotalDivCountDuration() {
        final Duration duration = getDuration();
        return duration == null ? null : duration.dividedBy(totalCount);
    }

    // nulable
    public static Duration getResponseMinDuration() {
        final OptionalLong millis = pairs.stream()
                .mapToLong(Pair::durationMillis)
                .min();
        return millis.isPresent() ? Duration.ofMillis(millis.getAsLong()) : null;
    }

    // nulable
    public static Duration getResponseAvgDuration() {
        final OptionalDouble millis = pairs.stream()
                .mapToLong(Pair::durationMillis)
                .average();
        return millis.isPresent() ? Duration.ofMillis((long) millis.getAsDouble()) : null;
    }

    // nulable
    public static Duration getResponseMedDuration() {
        final OptionalDouble millis = pairs.stream()
                .mapToLong(Pair::durationMillis)
                .sorted()
                .skip((pairs.size() - 1) / 2).limit(2 - pairs.size() % 2)
                .average();
        return millis.isPresent() ? Duration.ofMillis((long) millis.getAsDouble()) : null;
    }

    // nulable
    public static Duration getResponseMaxDuration() {
        final OptionalLong millis = pairs.stream()
                .mapToLong(Pair::durationMillis)
                .max();
        return millis.isPresent() ? Duration.ofMillis(millis.getAsLong()) : null;
    }

    public static void registerRequestProducer(final Object producer) {
        requestProducers.put(System.identityHashCode(producer), Instant.now());
    }

    public static void unregisterRequestProducer(final Object producer) {
        requestProducers.remove(System.identityHashCode(producer));
    }

    public static boolean isAllProduced() {
        return requestProducers.isEmpty();
    }

}
