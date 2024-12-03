package io.github.cyrilsochor.kafky.core.writer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.function.Supplier;

public interface StatisticsWriter {

    enum Flag {
        ALIGN_LEFT,
        ALIGN_CENTER,
        ALIGN_RIGHT,
        ;

        public Set<Flag> getFlagGroup() {
            // simple solution - all flags are in one group for now
            return Set.of(Flag.ALIGN_LEFT, Flag.ALIGN_CENTER, Flag.ALIGN_RIGHT);
        }
    }


    void open() throws IOException;

    void close();

    Path getPath();

    void createRecord();

    void finishRecord();

    void writeString(String stat, Supplier<String> valueSupplier, Flag... flags);

    void writeInteger(String stat, Supplier<Integer> valueSupplier, Flag... flags);

    void writeLong(String stat, Supplier<Long> valueSupplier, Flag... flags);

    void writeInstant(String stat, Supplier<Instant> valueSupplier, Flag... flags);

    void writeLocalDateTime(String stat, Supplier<LocalDateTime> valueSupplier, Flag... flags);

    void writeDuration(String stat, Supplier<Duration> valueSupplier, ChronoUnit truncateTo, Flag... flags);

}
