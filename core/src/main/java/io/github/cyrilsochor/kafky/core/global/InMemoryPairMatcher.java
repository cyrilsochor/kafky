package io.github.cyrilsochor.kafky.core.global;

import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIR_STATISTICS;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIR_STATISTICS_SIZE;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIR_STATISTICS_SUFFIX;
import static io.github.cyrilsochor.kafky.core.writer.StatisticsWriter.Flag.ALIGN_RIGHT;
import static java.lang.Math.max;
import static java.lang.Math.min;

import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.core.runtime.Runtime;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import io.github.cyrilsochor.kafky.core.writer.MarkdownTableStatisticsWriter;
import io.github.cyrilsochor.kafky.core.writer.StatisticsWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPairMatcher implements Component, PairMatcher {

    private final Logger LOG = LoggerFactory.getLogger(InMemoryPairMatcher.class);

    protected class Pair {
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

    protected static record MessageReference(String topic, int partition, long offset) {
    }

    public static InMemoryPairMatcher of(final Map<Object, Object> cfg, final Runtime runtime) {
        final Path statisticsPath = PropertiesUtils.getPath(cfg, PAIR_STATISTICS, PAIR_STATISTICS_SUFFIX);
        final List<String> statisticsSize = PropertiesUtils.getListOfStrings(cfg, PAIR_STATISTICS_SIZE);
        return new InMemoryPairMatcher(runtime, statisticsPath, statisticsSize);
    }

    protected final Runtime runtime;
    protected final StatisticsWriter statisticsWriter; // nullable
    protected final List<String> statisticsSize;

    protected final Map<String, Pair> openPairs = new HashMap<>();
    protected final Collection<Pair> finishedPairs = new ArrayList<>();
    protected final Set<MessageReference> ingoing = ConcurrentHashMap.newKeySet();
    protected long totalCount = 0;

    protected long firstRequestTimestamp = Long.MAX_VALUE;
    protected long lastRequestTimestamp = Long.MIN_VALUE;
    protected long firstResponseTimestamp = Long.MAX_VALUE;
    protected long lastResponseTimestamp = Long.MIN_VALUE;
    protected List<Long> responseDurationsMs = new ArrayList<>();
    protected Map<Integer, Instant> requestProducers = new ConcurrentHashMap<>();

    protected InMemoryPairMatcher(
            final Runtime runtime,
            final Path statisticsPath,
            final List<String> statisticsSize) {
        this.runtime = runtime;
        this.statisticsWriter = statisticsPath == null ? null : new MarkdownTableStatisticsWriter(statisticsPath);
        this.statisticsSize = statisticsSize;
    }

    @Override
    public void addProducedRequest(final String key, final ProducedRecord record) {
        synchronized (openPairs) {
            totalCount++;

            final Pair pair = new Pair();
            pair.key = key;
            pair.request = record;
            openPairs.put(key, pair);
        }
    }

    @Override
    public void addResponse(final String key, final ConsumerRecord<Object, Object> record, final boolean warmup) {
        synchronized (openPairs) {
            final Pair pair = openPairs.remove(key);
            if (pair == null) {
                LOG.debug("Unmatched response {}: {}", key, record);
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

    @Override
    public void addIngoing(final ConsumerRecord<Object, Object> inputMessage) {
        final String topic = inputMessage.topic();
        final int partition = inputMessage.partition();
        final long offset = inputMessage.offset();
        LOG.debug("Consumed {}#{}#{}", topic, partition, offset);
        ingoing.add(new MessageReference(topic, partition, offset));
    }

    @Override
    public boolean isAllPaired() {
        return openPairs.isEmpty();
    }

    @Override
    public void shutdownHook() {
        if (statisticsWriter != null) {
            LOG.info("Writing statistics to {}", statisticsWriter.getPath().toAbsolutePath());

            try {
                statisticsWriter.open();
                statisticsWriter.createRecord();
                statisticsWriter.writeInstant("Start", runtime::getStart);
                statisticsWriter.writeInstant("Finish", runtime::getFinish);
                statisticsWriter.writeString("User", runtime::getUser);
                statisticsWriter.writeString("Size", this::getSize, ALIGN_RIGHT);
                statisticsWriter.writeString("Ailments", this::getAilments, ALIGN_RIGHT);
                statisticsWriter.writeDuration("Test duration", this::getTestDuration, ChronoUnit.SECONDS);
                statisticsWriter.writeLong("Throughput /m", this::getThroughputPerMinute);
                statisticsWriter.writeDuration("Duration t/c", this::getTotalDivCountDuration, ChronoUnit.MILLIS);
                statisticsWriter.writeDuration("Duration min", this::getResponseMinDuration, ChronoUnit.MILLIS);
                statisticsWriter.writeDuration("Duration avg", this::getResponseAvgDuration, ChronoUnit.MILLIS);
                statisticsWriter.writeDuration("Duration med", this::getResponseMedDuration, ChronoUnit.MILLIS);
                statisticsWriter.writeDuration("Duration max", this::getResponseMaxDuration, ChronoUnit.MILLIS);
                statisticsWriter.writeString("Description", () -> null);
                statisticsWriter.finishRecord();
            } catch (Exception e) {
                LOG.error("Error write statistic to {}", statisticsWriter.getPath(), e);
            } finally {
                statisticsWriter.close();
            }
        }
    }

    protected LocalDateTime getTestStart() {
        return firstRequestTimestamp == Long.MAX_VALUE ? null
                : Instant.ofEpochMilli(firstRequestTimestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
    }

    protected LocalDateTime getTestFinish() {
        return lastResponseTimestamp == Long.MIN_VALUE ? null
                : Instant.ofEpochMilli(lastResponseTimestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
    }

    protected long getTestCount() {
        return totalCount;
    }

    protected String getSize() {
        final StringBuilder size = new StringBuilder();

        if (!statisticsSize.isEmpty()) {
            for (String s : statisticsSize) {
                size.append(s);
                size.append(" * ");
            }
        }

        size.append(getTestCount());

        return size.toString();
    }

    protected Duration getTestDuration() {
        return lastResponseTimestamp == Long.MIN_VALUE || firstRequestTimestamp == Long.MAX_VALUE ? null
                : Duration.ofMillis(lastResponseTimestamp - firstRequestTimestamp);
    }

    protected Long getThroughputPerMinute() {
        final Duration duration = getTestDuration();
        return duration == null || duration.isZero() ? null
                : totalCount * 1000 * 60 / duration.toMillis();
    }

    protected Duration getTotalDivCountDuration() {
        final Duration duration = getTestDuration();
        return duration == null ? null : duration.dividedBy(totalCount);
    }

    protected Duration getResponseMinDuration() {
        final OptionalLong millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .min();
        return millis.isPresent() ? Duration.ofMillis(millis.getAsLong()) : null;
    }

    protected Duration getResponseAvgDuration() {
        final OptionalDouble millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .average();
        return millis.isPresent() ? Duration.ofMillis((long) millis.getAsDouble()) : null;
    }

    protected Duration getResponseMedDuration() {
        final OptionalDouble millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .sorted()
                .skip((finishedPairs.size() - 1) / 2).limit(2 - finishedPairs.size() % 2)
                .average();
        return millis.isPresent() ? Duration.ofMillis((long) millis.getAsDouble()) : null;
    }

    protected Duration getResponseMaxDuration() {
        final OptionalLong millis = finishedPairs.stream()
                .mapToLong(p -> p.durationMillis)
                .max();
        return millis.isPresent() ? Duration.ofMillis(millis.getAsLong()) : null;
    }

    // nullable
    protected Integer getPassersByCount() {
        LOG.debug("Inputs {}, pairs {}", ingoing.size(), finishedPairs.size());

        if (ingoing.isEmpty()) {
            return null;
        } else {
            final Set<MessageReference> passersBy = new HashSet<>();
            passersBy.addAll(ingoing);
            for (final Pair p : finishedPairs) {
                passersBy.remove(new MessageReference(
                        p.request.metadata().topic(),
                        p.request.metadata().partition(),
                        p.request.metadata().offset()));
            }
            final int count = passersBy.size();
            LOG.debug("Detected passers by: {} {}", count, passersBy);
            return count;
        }
    }

    protected String getAilments() {
        final StringBuffer ailments = new StringBuffer();

        final Integer passersBy = getPassersByCount();
        if (passersBy == null) {
            ailments.append("? passers by");
        } else if (passersBy > 0) {
            ailments.append(passersBy);
            ailments.append(" passers by");
        }

        return ailments.toString();
    }

}
