package io.github.cyrilsochor.kafky.core.writer;

import static io.github.cyrilsochor.kafky.core.writer.StatisticsWriter.Flag.*;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.stream.Collectors.toSet;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class MarkdownTableStatisticsWriter implements StatisticsWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MarkdownTableStatisticsWriter.class);

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

    protected final Path path;

    protected BufferedWriter writer;
    protected boolean writeHeader;
    protected List<Statistic> currentRecord;

    public MarkdownTableStatisticsWriter(final Path path) {
        this.path = path;
    }

    @Override
    public void open() throws IOException {
        final boolean exists = Files.exists(path);
        writeHeader = !exists;
        writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException("Error close file " + path.toAbsolutePath());
            }
        }
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public void createRecord() {
        currentRecord = new ArrayList<>();
    }

    @Override
    public void finishRecord() {
        if (writeHeader) {
            writeHeader();
            writeHeader = false;
        }
        writeRecord();
        writer.flush();
        currentRecord = null;
    }

    protected void writeHeader() {
    }

    protected void writeRecord() {
    }

    protected void addStatistic(final String stat, final Supplier<String> supplier, final Set<Flag> flags) {
        String text;
        try {
            text = supplier.get();
        } catch (Exception e) {
            LOG.error("Statistics '" + stat + "' error", e);
            text = "ERR";
        }
        currentRecord.add(new Statistic(stat, text, flags));
    }

    protected void addStatistic(
            final String stat,
            final Supplier<String> supplier,
            final Flag[] highPriorityFlags,
            final Flag... lowPriorityFlags) {
        final Set<Flag> mergedFlags = new HashSet<>();
        for (Flag flag : lowPriorityFlags) {
            addFlag(mergedFlags, flag);
        }
        for (Flag flag : highPriorityFlags) {
            addFlag(mergedFlags, flag);
        }
        addStatistic(stat, supplier, mergedFlags);
    }

    protected void addFlag(final Set<Flag> flags, final Flag flag) {
        flags.add(flag);    
        odstranit konflitkni
    }

    @Override
    public void writeString(final String stat, final Supplier<String> valueSupplier, final Flag... flags) {
        addStatistic(stat, valueSupplier, flags, ALIGN_CENTER);
    }

    @Override
    public void writeInteger(final String stat, final Supplier<Integer> valueSupplier, final Flag... flags) {
        addStatistic(stat, () -> {
            final Integer v = valueSupplier.get();
            return v == null ? null : DECIMAL_FORMAT.format(v);
        }, flags, ALIGH_RIGHT);
    }

    @Override
    public void writeLong(final String stat, final Supplier<Long> valueSupplier, final Flag... flags) {
        addStatistic(stat, () -> {
            final Long v = valueSupplier.get();
            return v == null ? null : DECIMAL_FORMAT.format(v);
        }, flags, ALIGH_RIGHT);
    }

    @Override
    public void writeInstant(final String stat, final Supplier<Instant> valueSupplier, final Flag... flags) {
        addStatistic(stat, () -> {
            final Instant v = valueSupplier.get();
            return v == null ? null
                    : DATE_TIME_FORMATTER.format(v
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime());
        }, flags, ALIGN_LEFT);
    }

    @Override
    public void writeLocalDateTime(final String stat, final Supplier<LocalDateTime> valueSupplier, final Flag... flags) {
        addStatistic(stat, () -> {
            final LocalDateTime v = valueSupplier.get();
            return v == null ? null : DATE_TIME_FORMATTER.format(v);
        }, flags, ALIGN_LEFT);
    }

    @Override
    public void writeDuration(final String stat, final Supplier<Duration> valueSupplier, final ChronoUnit truncateTo, final Flag... flags) {
        addStatistic(stat, () -> {
            final Duration v = valueSupplier.get();
            if (v == null) {
                return null;
            } else {
                final Duration trunc = truncateTo == null ? v : v.truncatedTo(truncateTo);
                return trunc.toString().substring(2).toLowerCase();
            }
        }, flags, ALIGH_RIGHT);
    }

}
