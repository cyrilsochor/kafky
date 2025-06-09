package io.github.cyrilsochor.kafky.core.global;

import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIRS_OUTPUT_FILE;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIRS_OUTPUT_REQUEST_HEADERS;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIRS_OUTPUT_REQUEST_VALUE_PATHS;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIRS_OUTPUT_RESPONSE_HEADERS;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIRS_OUTPUT_RESPONSE_VALUE_PATHS;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIR_STATISTICS;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIR_STATISTICS_SIZE;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PAIR_STATISTICS_SUFFIX;
import static io.github.cyrilsochor.kafky.core.config.KafkyPairMatcherConfig.PASSERS_BY_OUTPUT_FILE;
import static io.github.cyrilsochor.kafky.core.report.Report.MESSAGES_COUNT_FORMAT;
import static io.github.cyrilsochor.kafky.core.util.Assert.assertFalse;
import static io.github.cyrilsochor.kafky.core.writer.StatisticsWriter.Flag.ALIGN_RIGHT;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.api.job.JobState;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.core.runtime.KafkyRuntime;
import io.github.cyrilsochor.kafky.core.stats.Statistics;
import io.github.cyrilsochor.kafky.core.storage.mapper.StorageSerializer;
import io.github.cyrilsochor.kafky.core.storage.text.TextWriter;
import io.github.cyrilsochor.kafky.core.util.FileUtils;
import io.github.cyrilsochor.kafky.core.util.InfoUtils;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import io.github.cyrilsochor.kafky.core.writer.MarkdownTableStatisticsWriter;
import io.github.cyrilsochor.kafky.core.writer.StatisticsWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPairMatcher implements Component, PairMatcher {

    private final Logger LOG = LoggerFactory.getLogger(InMemoryPairMatcher.class);
    protected static final Comparator<Pair> PAIRS_OUTPUT_COMPARATOR = Comparator.comparing((Pair p) -> p.phase.name())
            .thenComparingLong(p -> -p.durationMillis)
            .thenComparingLong(p -> p.request.sequenceNumber());

    protected static class Pair {
        protected String key;
        protected JobState phase;
        protected long pairTimestamp;
        protected long durationMillis;
        protected ProducedRecord request;
        protected ConsumerRecord<Object, Object> response;

        public Pair(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "Pair [key=" + key + ", phase=" + phase + ", pairTimestamp=" + pairTimestamp + ", durationMillis=" + durationMillis + "]";
        }

    }

    protected static record MessageReference(String topic, int partition, long offset) {
    }

    public record TopicPartition(String topic, int partition) {
    }

    protected static class PhaseStatistic {
        protected final JobState phase;
        protected long firstRequestTimestamp = Long.MAX_VALUE;
        protected long lastRequestTimestamp = Long.MIN_VALUE;
        protected long firstResponseTimestamp = Long.MAX_VALUE;
        protected long lastResponseTimestamp = Long.MIN_VALUE;
        protected ArrayList<Long> durations = new ArrayList<>();

        protected PhaseStatistic(final JobState phase) {
            this.phase = phase;
        }

        // nullable
        protected Duration getDuration() {
            return lastResponseTimestamp == Long.MIN_VALUE || firstRequestTimestamp == Long.MAX_VALUE ? null
                    : Duration.ofMillis(lastResponseTimestamp - firstRequestTimestamp);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("PhaseStatistic [phase=");
            builder.append(phase);
            builder.append(", size=");
            builder.append(durations.size());
            builder.append(", duration=");
            builder.append(getDuration());
            builder.append(", firstRequestTimestamp=");
            builder.append(firstRequestTimestamp);
            builder.append(", lastRequestTimestamp=");
            builder.append(lastRequestTimestamp);
            builder.append(", firstResponseTimestamp=");
            builder.append(firstResponseTimestamp);
            builder.append(", lastResponseTimestamp=");
            builder.append(lastResponseTimestamp);
            builder.append(", durations=");
            builder.append(durations);
            builder.append("]");
            return builder.toString();
        }

    }

    public static InMemoryPairMatcher of(final Map<Object, Object> cfg, final KafkyRuntime runtime) throws IOException {
        final Path statisticsPath = PropertiesUtils.getPath(cfg, PAIR_STATISTICS, PAIR_STATISTICS_SUFFIX);
        final Path pairsOutputPath = PropertiesUtils.getPath(cfg, PAIRS_OUTPUT_FILE);
        final List<String> statisticsSize = PropertiesUtils.getListOfStrings(cfg, PAIR_STATISTICS_SIZE);

        final Path passersByOutputPath = PropertiesUtils.getPath(cfg, PASSERS_BY_OUTPUT_FILE);
        final List<String> pairsOuputRequestHeaders = PropertiesUtils.getListOfStrings(cfg, PAIRS_OUTPUT_REQUEST_HEADERS);
        final List<String> pairsOuputResponseHeaders = PropertiesUtils.getListOfStrings(cfg, PAIRS_OUTPUT_RESPONSE_HEADERS);
        final List<List<String>> pairsOuputRequestValuePaths = parseFieldPaths(cfg, PAIRS_OUTPUT_REQUEST_VALUE_PATHS);
        final List<List<String>> pairsOuputResponseValuePaths = parseFieldPaths(cfg, PAIRS_OUTPUT_RESPONSE_VALUE_PATHS);
        return new InMemoryPairMatcher(
                runtime,
                statisticsPath,
                pairsOutputPath,
                passersByOutputPath,
                statisticsSize,
                pairsOuputRequestHeaders,
                pairsOuputResponseHeaders,
                pairsOuputRequestValuePaths,
                pairsOuputResponseValuePaths);
    }

    protected static List<List<String>> parseFieldPaths(final Map<Object, Object> cfg, final String cfgKey) {
        return PropertiesUtils.getListOfStrings(cfg, cfgKey).stream()
                .map(p -> Arrays.asList(p.split("\\.")))
                .toList();
    }

    protected final KafkyRuntime runtime;
    protected final StatisticsWriter statisticsWriter; // nullable
    protected final Path pairsOutputPath; //nullable
    protected final Path passersByOutputPath; //nullable
    protected final List<String> statisticsSize;
    protected final List<String> pairsOuputRequestHeaders;
    protected final List<String> pairsOuputResponseHeaders;
    protected final List<List<String>> pairsOuputRequestValuePaths;
    protected final List<List<String>> pairsOuputResponseValuePaths;

    protected final Map<String, Pair> openPairs = new HashMap<>();
    protected final Collection<Pair> finishedPairs = new ArrayList<>();
    protected final Map<MessageReference, ConsumerRecord<Object, Object>> ingoing = new ConcurrentHashMap<>();
    protected long totalCount = 0;

    protected List<Long> responseDurationsMs = new ArrayList<>();
    protected Map<Integer, Instant> requestProducers = new ConcurrentHashMap<>();

    protected Map<TopicPartition, Long> processorStartOffsets = new HashMap<>();
    protected Map<TopicPartition, Long> processorFinishOffsets = new HashMap<>();

    protected InMemoryPairMatcher(
            final KafkyRuntime runtime,
            final Path statisticsPath,
            final Path pairsByOutputPath,
            final Path passersByOutputPath,
            final List<String> statisticsSize,
            final List<String> pairsOuputRequestHeaders,
            final List<String> pairsOuputResponseHeaders,
            final List<List<String>> pairsOuputRequestValuePaths,
            final List<List<String>> pairsOuputResponseValuePaths) throws IOException {
        this.runtime = runtime;
        this.statisticsWriter = statisticsPath == null ? null : new MarkdownTableStatisticsWriter(statisticsPath);
        this.pairsOutputPath = pairsByOutputPath;
        this.passersByOutputPath = passersByOutputPath;
        this.statisticsSize = statisticsSize;
        this.pairsOuputRequestHeaders = pairsOuputRequestHeaders;
        this.pairsOuputResponseHeaders = pairsOuputResponseHeaders;
        this.pairsOuputRequestValuePaths = pairsOuputRequestValuePaths;
        this.pairsOuputResponseValuePaths = pairsOuputResponseValuePaths;
    }

    @Override
    public void addProducedRequest(final String key, final ProducedRecord record) {
        synchronized (openPairs) {
            LOG.debug("Adding request {}", key);
            totalCount++;

            final Pair pair = openPairs.computeIfAbsent(key, k -> new Pair(key));
            pair.request = record;

            finishPaired(pair);
        }
    }

    @Override
    public void addResponse(final String key, final ConsumerRecord<Object, Object> record, final JobState jobState) {
        synchronized (openPairs) {
            LOG.debug("Adding response {}", key);

            final Pair pair = openPairs.computeIfAbsent(key, k -> new Pair(key));
            pair.response = record;
            pair.phase = jobState;

            finishPaired(pair);
        }
    }

    protected void finishPaired(final Pair pair) {
        if (pair.request == null) {
            LOG.debug("Pair {} is still open - request is null", pair.key);
        } else if (pair.response == null) {
            LOG.debug("Pair {} is still open - response is null", pair.key);
        } else {
            pair.pairTimestamp = System.currentTimeMillis(); // don't use record.timestam() - it's producer timestamp with different clock
            pair.durationMillis = pair.pairTimestamp - pair.request.metadata().timestamp();

            LOG.debug("Pair {} is finished: {}", pair.key, pair);
            openPairs.remove(pair.key);
            finishedPairs.add(pair);
        }
    }

    @Override
    public void addIngoing(final ConsumerRecord<Object, Object> inputMessage) {
        final String topic = inputMessage.topic();
        final int partition = inputMessage.partition();
        final long offset = inputMessage.offset();
        LOG.debug("Consumed {}#{}#{}", topic, partition, offset);
        ingoing.put(new MessageReference(topic, partition, offset), inputMessage);
    }

    @Override
    public void setProcesserStartOffset(final String topic, final int partition, final long offset) {
        LOG.trace("setProcesserStartOffset {}#{}: {}", topic, partition, offset);
        processorStartOffsets.put(new TopicPartition(topic, partition), offset);
    }

    @Override
    public void setProcesserFinishOffset(final String topic, final int partition, final long offset) {
        LOG.trace("setProcesserFinishOffset {}#{}: {}", topic, partition, offset);
        processorFinishOffsets.put(new TopicPartition(topic, partition), offset);
    }

    @Override
    public boolean areAllRequestsPaired() {
        return 0 == getUnpairedRequestsCount();
    }

    public long getUnpairedRequestsCount() {
        synchronized (openPairs) {
            return openPairs.values().stream()
                    .filter(p -> p.request != null)
                    .count();
        }
    }

    @Override
    public String getReport() {
        final long openCount = getUnpairedRequestsCount();
        if (openCount != 0 || !finishedPairs.isEmpty()) {
            return format("open: " + MESSAGES_COUNT_FORMAT, openCount);
        } else {
            return null;
        }
    }

    @Override
    public void shutdownHook() {
        if (pairsOutputPath != null) {
            writePairs();
        }

        if (passersByOutputPath == null && statisticsWriter == null) {
            return;
        }

        final var passersBy = cookPassertsBy();
        if (passersByOutputPath != null) {
            writePassersBy(passersBy);
        }

        if (statisticsWriter != null) {
            final var stats = cookStatistics(passersBy);
            writeStatistics(stats);
        }
    }

    protected void writeStatistics(final Statistics stats) {
        LOG.info("Writing statistics to {}", statisticsWriter.getPath().toAbsolutePath());

        try {
            statisticsWriter.open();
            statisticsWriter.createRecord();
            statisticsWriter.writeInstant("Start", stats::getStart);
            statisticsWriter.writeInstant("Finish", stats::getFinish);
            statisticsWriter.writeString("User", stats::getUser);
            statisticsWriter.writeString("Size", stats::getSize, ALIGN_RIGHT);
            statisticsWriter.writeString("Ailments", stats::getAilments, ALIGN_RIGHT);
            statisticsWriter.writeDuration("Test duration", stats::getTestDuration, ChronoUnit.SECONDS);
            statisticsWriter.writeLong("Throughput /m", stats::getThroughputPerMinute);
            statisticsWriter.writeDuration("Duration t/c", stats::getTotalDivCountDuration, ChronoUnit.MILLIS);
            statisticsWriter.writeDuration("Duration min", stats::getResponseMinDuration, ChronoUnit.MILLIS);
            statisticsWriter.writeDuration("Duration avg", stats::getResponseAvgDuration, ChronoUnit.MILLIS);
            statisticsWriter.writeDuration("Duration med", stats::getResponseMedDuration, ChronoUnit.MILLIS);
            statisticsWriter.writeDuration("Duration max", stats::getResponseMaxDuration, ChronoUnit.MILLIS);
            statisticsWriter.writeString("Description", () -> null);
            statisticsWriter.finishRecord();
        } catch (Exception e) {
            LOG.error("Error write statistic to {}", statisticsWriter.getPath(), e);
        } finally {
            statisticsWriter.close();
        }
    }

    protected void writePairs() {
        LOG.info("Writing pairs to {}", pairsOutputPath.toAbsolutePath());

        try (Writer writer = FileUtils.createWriter(pairsOutputPath)) {

            writer.append("Seq");
            writer.append(",");
            writer.append("Phase");
            writer.append(",");
            writer.append("MessageId");
            writer.append(",");
            for (final String key : pairsOuputRequestHeaders) {
                writer.append("Request " + key);
                writer.append(",");
            }
            for (final List<String> fieldPath : pairsOuputRequestValuePaths) {
                writer.append("Request " + fieldPath.stream().collect(joining(".")));
                writer.append(",");
            }
            for (final String key : pairsOuputResponseHeaders) {
                writer.append("Response " + key);
                writer.append(",");
            }
            for (final List<String> fieldPath : pairsOuputResponseValuePaths) {
                writer.append("Response " + fieldPath.stream().collect(joining(".")));
                writer.append(",");
            }
            writer.append("Duration (ms)");
            writer.append(System.lineSeparator());

            for (final Pair p : finishedPairs.stream()
                    .sorted(PAIRS_OUTPUT_COMPARATOR)
                    .toList()) {
                writer.append(Long.toString(p.request.sequenceNumber()));
                writer.append(",");
                writer.append(p.phase.name());
                writer.append(",");
                writer.append(p.key);
                writer.append(",");
                for (final String key : pairsOuputRequestHeaders) {
                    writeHeader(writer, p.request.record().headers(), key);
                }
                for (final List<String> fieldPath : pairsOuputRequestValuePaths) {
                    writeValueField(writer, (GenericRecord) p.request.record().value(), fieldPath);
                }
                for (final String key : pairsOuputResponseHeaders) {
                    writeHeader(writer, p.response.headers(), key);
                }
                for (final List<String> fieldPath : pairsOuputResponseValuePaths) {
                    writeValueField(writer, (GenericRecord) p.response.value(), fieldPath);
                }
                writer.append(Long.toString(p.durationMillis));
                writer.append(System.lineSeparator());
            }
        } catch (Exception e) {
            LOG.error("Error write pairs", e);
        }

        LOG.debug("Writing pairs to {} finished", pairsOutputPath.toAbsolutePath());
    }

    protected void writeHeader(final Writer writer, final Headers headers, final String key) throws IOException {
        final Header header = headers.lastHeader(key);
        if (header != null) {
            writer.append(new String(header.value(), UTF_8));
        }
        writer.append(",");
    }

    protected void writeValueField(final Writer writer, final GenericRecord value, final List<String> fieldPath) throws IOException {
        String valueString = null;
        try {
            final Object v = getValueField(value, fieldPath);
            if (v != null) {
                valueString = v.toString();
            }
        } catch (final Exception e) {
            valueString = "ERROR: " + e.getMessage();
        }
        writer.append(valueString);
        writer.append(",");
    }

    protected Object getValueField(final GenericRecord value, final List<String> path) {
        assertFalse(path.isEmpty(), "Expected non-empty path");
        final String field = path.get(0);
        if (path.size() == 1) {
            return value.get(field);
        } else {
            final GenericRecord subValue = (GenericRecord) value.get(field);
            return getValueField(subValue, path.subList(1, path.size()));
        }
    }

    protected void writePassersBy(final Map<MessageReference, ConsumerRecord<Object, Object>> passersBy) {
        LOG.info("Writing passers by to {}", passersByOutputPath.toAbsolutePath());

        try {
            final StorageSerializer passersBySerializer = new StorageSerializer(new TextWriter(passersByOutputPath));
            try {
                passersBySerializer.init();
                for (ConsumerRecord<Object, Object> p : passersBy.values()) {
                    passersBySerializer.consume(p);
                }
            } finally {
                passersBySerializer.close();
            }
        } catch (Exception e) {
            LOG.error("Error write passers by", e);
        }
        LOG.debug("Writing passers by to {} finished", passersByOutputPath.toAbsolutePath());
    }

    protected Map<MessageReference, ConsumerRecord<Object, Object>> cookPassertsBy() {
        if (ingoing.isEmpty()) {
            LOG.debug("Passers by detection is not enabled");
            return emptyMap();
        } else {
            LOG.debug("Inputs {}, pairs {}", ingoing.size(), finishedPairs.size());
            final Map<MessageReference, ConsumerRecord<Object, Object>> passersBy = new TreeMap<>(
                    Comparator.comparing(MessageReference::topic)
                            .thenComparing(MessageReference::partition)
                            .thenComparing(MessageReference::offset));
            for (final TopicPartition tp : processorStartOffsets.keySet()) {
                final Long startOffset = processorStartOffsets.get(tp);
                final Long finishOffset = processorFinishOffsets.get(tp);
                if (startOffset != null && finishOffset != null) {
                    final long offsetChange = finishOffset - startOffset;
                    LOG.debug("Processer offset change {}#{}: {} ({}->{})", tp.topic, tp.partition, offsetChange, startOffset, finishOffset);
                    for (long o = startOffset; o < finishOffset; o++) {
                        passersBy.put(
                                new MessageReference(tp.topic, tp.partition(), o),
                                new ConsumerRecord<Object, Object>(tp.topic, tp.partition, o, null, null));
                    }
                }
            }
            ingoing.forEach((mr, r) -> {
                passersBy.put(mr, r);
            });
            for (final Pair p : finishedPairs) {
                passersBy.remove(new MessageReference(
                        p.request.metadata().topic(),
                        p.request.metadata().partition(),
                        p.request.metadata().offset()));
            }
            final int count = passersBy.size();
            LOG.debug("Detected {} passers by", count);
            return passersBy;
        }
    }

    protected Statistics cookStatistics(final Map<MessageReference, ConsumerRecord<Object, Object>> passersBy) {

        final Statistics stats = new Statistics();

        stats.setStart(runtime.getStart());
        stats.setFinish(runtime.getFinish());
        stats.setUser(runtime.getUser());

        final StringBuilder sizeBuilder = new StringBuilder();
        if (!statisticsSize.isEmpty()) {
            for (String s : statisticsSize) {
                sizeBuilder.append(s);
                sizeBuilder.append(" * ");
            }
        }
        sizeBuilder.append(totalCount);
        stats.setSize(sizeBuilder.toString());

        final StringBuilder ailmentsBuilder = new StringBuilder();
        final long unpairedRequestsCount = getUnpairedRequestsCount();
        if (unpairedRequestsCount > 0) {
            InfoUtils.appendSentence(ailmentsBuilder, unpairedRequestsCount + " without response");
        }
        if (passersBy.size() > 0) {
            InfoUtils.appendSentence(ailmentsBuilder, passersBy.size() + " passers by");
        }
        stats.setAilments(ailmentsBuilder.toString());

        final Map<JobState, PhaseStatistic> phasesStats = cookPhasesStatistics();
        cookOverallStatistics(stats, phasesStats);
        phasesStats.values().forEach(ps -> LOG.debug("Phase statistics: {}", ps));

        final PhaseStatistic responseTimeStats = phasesStats.get(JobState.MEASURING_RESPONSE_TIME);
        if (responseTimeStats != null) {
            cookResponseTimeStatistics(stats, responseTimeStats);
        }

        final PhaseStatistic measuringThroughputStats = phasesStats.get(JobState.MEASURING_THROUGHPUT);
        if (measuringThroughputStats != null) {
            cookThroughputStaistics(stats, measuringThroughputStats);
        }

        return stats;
    }

    protected Map<JobState, PhaseStatistic> cookPhasesStatistics() {
        final Map<JobState, PhaseStatistic> phasesStats = new TreeMap<>();
        for (final Pair pair : finishedPairs) {
            final PhaseStatistic phaseStats = phasesStats.computeIfAbsent(pair.phase, PhaseStatistic::new);

            final long requestTimestamp = pair.request.metadata().timestamp();
            phaseStats.firstRequestTimestamp = min(phaseStats.firstRequestTimestamp, requestTimestamp);
            phaseStats.lastRequestTimestamp = max(phaseStats.lastRequestTimestamp, requestTimestamp);
            phaseStats.firstResponseTimestamp = min(phaseStats.firstResponseTimestamp, pair.pairTimestamp);
            phaseStats.lastResponseTimestamp = max(phaseStats.lastResponseTimestamp, pair.pairTimestamp);
            phaseStats.durations.add(pair.durationMillis);
        }
        return phasesStats;
    }

    protected void cookOverallStatistics(final Statistics stats, final Map<JobState, PhaseStatistic> phasesStats) {
        stats.setTestDuration(phasesStats.values().stream()
                .map(PhaseStatistic::getDuration)
                .reduce(Duration.ZERO, (d0, d1) -> d0.plus(d1)));
    }

    protected void cookResponseTimeStatistics(final Statistics stats, final PhaseStatistic responseTimeStats) {
        final int count = responseTimeStats.durations.size();
        final long[] sortedDurations = new long[count];
        long sum = 0;
        for (int i = 0; i < count; i++) {
            final Long d = responseTimeStats.durations.get(i);
            sortedDurations[i] = d;
            sum += d;
        }
        Arrays.sort(sortedDurations);

        stats.setResponseMinDuration(Duration.ofMillis(sortedDurations[0]));
        LOG.debug("Cooked responseMinDuration: {}", stats.getResponseMinDuration());
        stats.setResponseAvgDuration(Duration.ofMillis(sum / count));
        LOG.debug("Cooked responseAvgDuration: {}", stats.getResponseAvgDuration());
        stats.setResponseMedDuration(Duration.ofMillis(sortedDurations.length % 2 == 0 // ODD vs EVEN 
                ? (sortedDurations[sortedDurations.length / 2] + sortedDurations[sortedDurations.length / 2 - 1]) / 2
                : sortedDurations[sortedDurations.length / 2]));
        LOG.debug("Cooked responseMedDuration: {}", stats.getResponseMedDuration());
        stats.setResponseMaxDuration(Duration.ofMillis(sortedDurations[sortedDurations.length - 1]));
        LOG.debug("Cooked responseMaxDuration: {}", stats.getResponseMaxDuration());
    }

    protected void cookThroughputStaistics(final Statistics stats, final PhaseStatistic measuringThroughputStats) {
        final Duration duration = measuringThroughputStats.getDuration();
        if (duration != null && !duration.isZero()) {
            stats.setThroughputPerMinute(measuringThroughputStats.durations.size() * 1000 * 60 / duration.toMillis());
            stats.setTotalDivCountDuration(duration.dividedBy(measuringThroughputStats.durations.size()));
        }
    }

}
