package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static java.lang.String.format;

import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecord;
import io.github.cyrilsochor.kafky.api.job.producer.ProducedRecordListener;
import io.github.cyrilsochor.kafky.core.config.KafkyProducerConfig;
import io.github.cyrilsochor.kafky.core.util.FileUtils;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class ProducedRecordLog implements ProducedRecordListener {

    private static final Logger LOG = LoggerFactory.getLogger(ProducedRecordLog.class);

    public static ProducedRecordListener of(final Map<Object, Object> cfg) throws IOException {
        final Path producerLogPath = PropertiesUtils.getPath(cfg, KafkyProducerConfig.LOG_FILE);
        if (producerLogPath == null) {
            return null;
        }

        LOG.info("Writing produced messages info log to {}", producerLogPath.toAbsolutePath());

        return new ProducedRecordLog(FileUtils.createWriter(producerLogPath));

    }

    protected Writer writer;
    protected Instant start;

    public ProducedRecordLog(Writer writer) {
        super();
        this.writer = writer;
    }

    @Override
    public void init() throws Exception {
        start = Instant.now();
    }

    @Override
    public void consume(final ProducedRecord producedRecord) throws Exception {
        final Long elapsedTime = Duration.between(start, Instant.now()).toMillis();
        try {
            if (producedRecord.sendException() != null) {
                writer.append(format("%4dms: message #%04d exception-message=%s, thread=%s%n",
                        elapsedTime,
                        producedRecord.sequenceNumber(),
                        producedRecord.sendException().getMessage(),
                        Thread.currentThread().getName()));
            } else {
                writer.append(format("%4dms: message #%04d topic=%s, partition=%02d, offset=%04d, timestamp-utc=%s, thread=%s%n",
                        elapsedTime,
                        producedRecord.sequenceNumber(),
                        producedRecord.metadata().topic(),
                        producedRecord.metadata().partition(),
                        producedRecord.metadata().offset(),
                        Instant.ofEpochMilli(producedRecord.metadata().timestamp()),
                        Thread.currentThread().getName()));
            }
            writer.flush();
        } catch (IOException logException) {
            LOG.error("Error write producer log", logException);
        }
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }

}
