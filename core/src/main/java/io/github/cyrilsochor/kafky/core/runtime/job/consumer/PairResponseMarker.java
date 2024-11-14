package io.github.cyrilsochor.kafky.core.runtime.job.consumer;

import static io.github.cyrilsochor.kafky.api.job.JobState.WARMED;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import io.github.cyrilsochor.kafky.api.job.consumer.AbstractRecordConsumer;
import io.github.cyrilsochor.kafky.api.job.consumer.ConsumerJobStatus;
import io.github.cyrilsochor.kafky.core.config.KafkyConsumerConfig;
import io.github.cyrilsochor.kafky.core.pair.PairMatcher;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class PairResponseMarker extends AbstractRecordConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PairResponseMarker.class);

    private static final CharSequence RECORD_SEPARATOR = "\n";
    private static final CharSequence FIELD_SEPARATOR = "|";
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("# ###");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder() // like ISO_DATE_TIME, truncate to 3 digit millis 
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 3, 3, true)
            .toFormatter();

    public static PairResponseMarker of(final Map<Object, Object> cfg, final ConsumerJobStatus jobStatus) throws IOException {
        final String headerKey = PropertiesUtils.getString(cfg, KafkyConsumerConfig.PAIR_RESPONSE_HEADER);
        if (headerKey == null) {
            return null;
        }

        final Path statisticsPath = PropertiesUtils.getPath(cfg, KafkyConsumerConfig.PAIR_STATISTICS, KafkyConsumerConfig.PAIR_STATISTICS_SUFFIX);
        final List<String> statisticsSize = PropertiesUtils.getListOfStrings(cfg, KafkyConsumerConfig.PAIR_STATISTICS_SIZE);
        return new PairResponseMarker(jobStatus, headerKey, statisticsPath, statisticsSize);
    }

    protected final ConsumerJobStatus jobStatus;
    protected final String headerKey;
    protected final Path statisticsPath;
    protected final List<String> statisticsSize;

    public PairResponseMarker(
            final ConsumerJobStatus jobStatus,
            final String headerKey,
            final Path statisticsPath,
            final List<String> statisticsSize) {
        this.jobStatus = jobStatus;
        this.headerKey = headerKey;
        this.statisticsPath = statisticsPath;
        this.statisticsSize = statisticsSize;
    }

    @Override
    public void consume(final ConsumerRecord<Object, Object> consumerRecord) throws Exception {
        final Header header = consumerRecord.headers().lastHeader(headerKey);
        if (header != null) {
            final String key = new String(header.value());
            final boolean warmup = jobStatus.getRuntimeStatus().getMinProducerState().ordinal() <= WARMED.ordinal();
            PairMatcher.addResponse(key, consumerRecord, warmup);
        }

        getChainNext().consume(consumerRecord);
    }

    @Override
    public int getPriority() {
        return 10;
    }

    @Override
    public void shutdownHook() {
        if (statisticsPath != null) {
            LOG.info("Writing statistics to {}", statisticsPath.toAbsolutePath());

            final boolean exists = Files.exists(statisticsPath);
            try (final BufferedWriter writer = Files.newBufferedWriter(statisticsPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                if (!exists) {
                    writer.append(FIELD_SEPARATOR);
                    writeHeader(writer, "Start");
                    writeHeader(writer, "Finish");
                    writeHeader(writer, "User");
                    writeHeader(writer, "Size");
                    writeHeader(writer, "Passers by");
                    writeHeader(writer, "Test duration");
                    writeHeader(writer, "Throughput /m");
                    writeHeader(writer, "Duration t/c");
                    writeHeader(writer, "Duration min");
                    writeHeader(writer, "Duration avg");
                    writeHeader(writer, "Duration med");
                    writeHeader(writer, "Duration max");
                    writeHeader(writer, "Description");
                    writeRecordStart(writer);
                    writeHeader(writer, "----");
                    writeHeader(writer, "----");
                    writeHeader(writer, "----");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:"); // Test duration
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "---:");
                    writeHeader(writer, "----");
                }
                writeRecordStart(writer);
                writeFieldInstant(writer, jobStatus.getRuntimeStatus()::getStart);
                writeFieldInstant(writer, jobStatus.getRuntimeStatus()::getFinish);
                writeFieldString(writer, this::getUser);
                writeFieldString(writer, this::getSize);
                writeFieldLong(writer, PairMatcher::getPassersByCount);
                writeFieldDuration(writer, PairMatcher::getTestDuration, ChronoUnit.SECONDS);
                writeFieldLong(writer, PairMatcher::getThroughputPerMinute);
                writeFieldDuration(writer, PairMatcher::getTotalDivCountDuration, ChronoUnit.MILLIS);
                writeFieldDuration(writer, PairMatcher::getResponseMinDuration, ChronoUnit.MILLIS);
                writeFieldDuration(writer, PairMatcher::getResponseAvgDuration, ChronoUnit.MILLIS);
                writeFieldDuration(writer, PairMatcher::getResponseMedDuration, ChronoUnit.MILLIS);
                writeFieldDuration(writer, PairMatcher::getResponseMaxDuration, ChronoUnit.MILLIS);
                writeFieldString(writer, () -> null);
            } catch (IOException e) {
                LOG.error("Error write statistic to {}", statisticsPath, e);
            }
        }
    }

    // nullable
    protected String getUser() {
        final String rawUsername = System.getProperty("user.name");
        return rawUsername == null ? null : rawUsername.toLowerCase();
    }

    protected String getSize() {
        final StringBuilder size = new StringBuilder();

        if (!statisticsSize.isEmpty()) {
            for (String s : statisticsSize) {
                size.append(s);
                size.append(" * ");
            }
        }

        size.append(PairMatcher.getTestCount());

        return size.toString();
    }

    protected void writeHeader(final BufferedWriter writer, final String name) throws IOException {
        writer.append(name);
        writer.append(FIELD_SEPARATOR);
    }

    protected void writeRecordStart(final BufferedWriter writer) throws IOException {
        writer.append(RECORD_SEPARATOR);
        writer.append(FIELD_SEPARATOR);
    }

    protected void writeFieldString(final BufferedWriter writer, final Supplier<String> supplier) throws IOException {
        String v;
        try {
            v = supplier.get();
        } catch (Exception e) {
            LOG.error("Statistics error", e);
            v = "ERR";
        }
        if (v != null) {
            writer.append(v);
        }
        writer.append(FIELD_SEPARATOR);
    }

    protected void writeFieldLong(final BufferedWriter writer, final Supplier<Long> l) throws IOException {
        writeFieldString(writer, () -> {
            final Long v = l.get();
            return v == null ? null : DECIMAL_FORMAT.format(v);
        });
    }

    protected void writeFieldLocalDateTime(final BufferedWriter writer, final Supplier<LocalDateTime> t) throws IOException {
        writeFieldString(writer, () -> {
            final LocalDateTime v = t.get();
            return v == null ? null : DATE_TIME_FORMATTER.format(v);
        });
    }

    protected void writeFieldInstant(final BufferedWriter writer, final Supplier<Instant> t) throws IOException {
        writeFieldString(writer, () -> {
            final Instant v = t.get();
            return v == null ? null
                    : DATE_TIME_FORMATTER.format(v
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime());
        });
    }

    protected void writeFieldDuration(final BufferedWriter writer, final Supplier<Duration> d, final TemporalUnit truncateTo) throws IOException {
        writeFieldString(writer, () -> {
            final Duration v = d.get();
            if (v == null) {
                return null;
            } else {
                final Duration trunc = truncateTo == null ? v : v.truncatedTo(truncateTo);
                return trunc.toString().substring(2).toLowerCase();
            }
        });
    }

    private Instant getFinish() {
        return null;
    }

}
