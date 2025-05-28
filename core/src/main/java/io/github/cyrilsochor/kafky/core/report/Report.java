package io.github.cyrilsochor.kafky.core.report;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import io.github.cyrilsochor.kafky.core.config.KafkyReportConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Supplier;

public class Report {

    private static final Logger LOG = LoggerFactory.getLogger(Report.class);
    public static final String MESSAGES_COUNT_FORMAT = "%8d";
    protected static final DateTimeFormatter CONSOLE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static Report of(Properties cfg) {
        return new Report(
                PropertiesUtils.getLongRequired(cfg, KafkyReportConfig.JOBS_STATUS_PERIOD),
                PropertiesUtils.getBooleanRequired(cfg, KafkyReportConfig.SYSTEM_OUT),
                PropertiesUtils.getBooleanRequired(cfg, KafkyReportConfig.LOG));
    }

    protected final long jobsStatusPeriod;
    protected final boolean toSystemOut;
    protected final boolean toLog;

    protected Report(
            final long jobsStatusPeriod,
            final boolean toSystemOut,
            final boolean toLog) {
        this.jobsStatusPeriod = jobsStatusPeriod;
        this.toSystemOut = toSystemOut;
        this.toLog = toLog;
    }

    public long getJobsStatusPeriod() {
        return jobsStatusPeriod;
    }

    @SuppressWarnings("java:S106")
    protected void displayReport(final Supplier<String> messageSupplier, final Throwable throwable) {
        if (!toSystemOut && !toLog) {
            return;
        }

        final String message = messageSupplier.get();

        if (toSystemOut) {
            final StringBuilder consoleText = new StringBuilder();
            consoleText.append(CONSOLE_TIMESTAMP_FORMATTER.format(LocalDateTime.now()));
            consoleText.append(" ");
            consoleText.append(message);

            if (throwable != null) {
                final String throableMessage = throwable.getMessage();
                consoleText.append(", throwable ");
                consoleText.append(throwable.getClass().getName());
                if (isEmpty(throableMessage)) {
                    consoleText.append(": ");
                    consoleText.append(throableMessage);
                }
            }

            System.out.println(consoleText.toString());
        }

        if (toLog) {
            if (throwable == null) {
                LOG.info(message);
            } else {
                LOG.error(message, throwable);
            }
        }

    }

    public void report(final String messageFormat, final Object... messageArgs) {
        report(() -> format(messageFormat, messageArgs));
    }

    public void report(final String message) {
        report(() -> message);
    }

    public void report(final Supplier<String> messageSupplier) {
        displayReport(messageSupplier, null);
    }

    public void reportException(final Throwable throable, final String messageFormat, final Object... messageArgs) {
        reportException(throable, () -> format(messageFormat, messageArgs));
    }

    public void report(final Throwable throable, final String message) {
        reportException(throable, () -> message);
    }

    public void reportException(final Throwable throable, final Supplier<String> messageSupplier) {
        displayReport(messageSupplier, throable);
    }

}