package io.github.cyrilsochor.kafky.core.report;

import static java.lang.String.format;

import io.github.cyrilsochor.kafky.core.config.KafkyReportConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.function.Supplier;

public class Report {

    private static final Logger LOG = LoggerFactory.getLogger(Report.class);

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

    protected void displayReport(final Supplier<String> messageSupplier, final Throwable throwable) {
        if (!toSystemOut && !toLog) {
            return;
        }

        final String message = messageSupplier.get();

        if (toSystemOut) {
            if (throwable == null) {
                System.out.println(message);
            } else {
                final String throableMessage = throwable.getMessage();
                System.out.println(message
                        + ", throwable " + throwable.getClass().getName()
                        + (StringUtils.isEmpty(throableMessage) ? "" : ": " + throableMessage));
            }
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