package io.github.cyrilsochor.kafky.core.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

class MarkdownTableStatisticsWriterTest {

    private Path path;

    @BeforeEach
    void initPath() throws IOException {
        path = Files.createTempFile(MarkdownTableStatisticsWriterTest.class.getSimpleName(), ".md");
        Files.delete(path);
    }

    @AfterEach
    void cleanupPath() throws IOException {
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    @Test
    void testFirstRecord() throws IOException {
        final MarkdownTableStatisticsWriter statisticsWriter = new MarkdownTableStatisticsWriter(path);
        statisticsWriter.open();
        statisticsWriter.createRecord();
        statisticsWriter.writeString("Name", (Supplier<String>) () -> "Alzbeta");
        statisticsWriter.writeLong("Age", (Supplier<Long>) () -> 37l);
        statisticsWriter.writeLong("$", (Supplier<Long>) () -> 1234567l);
        statisticsWriter.finishRecord();
        statisticsWriter.close();

        assertEquals("""
                |Name|Age|$|
                |:---|---:|---:|
                |Alzbeta|37|1 234 567|""", Files.readString(path));
    }

}
