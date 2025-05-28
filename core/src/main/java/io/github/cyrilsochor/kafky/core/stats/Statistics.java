package io.github.cyrilsochor.kafky.core.stats;

import java.time.Duration;
import java.time.Instant;

public class Statistics {

    private Instant start;
    private Instant finish;
    private String user;
    private String size;
    private String ailments;
    private Duration testDuration;
    private Long throughputPerMinute;
    private Duration totalDivCountDuration;
    private Duration responseMinDuration;
    private Duration responseAvgDuration;
    private Duration responseMedDuration;
    private Duration responseMaxDuration;

    public Instant getStart() {
        return start;
    }

    public void setStart(Instant start) {
        this.start = start;
    }

    public Instant getFinish() {
        return finish;
    }

    public void setFinish(Instant finish) {
        this.finish = finish;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getAilments() {
        return ailments;
    }

    public void setAilments(String ailments) {
        this.ailments = ailments;
    }

    public Duration getTestDuration() {
        return testDuration;
    }

    public void setTestDuration(Duration testDuration) {
        this.testDuration = testDuration;
    }

    public Long getThroughputPerMinute() {
        return throughputPerMinute;
    }

    public void setThroughputPerMinute(Long throughputPerMinute) {
        this.throughputPerMinute = throughputPerMinute;
    }

    public Duration getTotalDivCountDuration() {
        return totalDivCountDuration;
    }

    public void setTotalDivCountDuration(Duration totalDivCountDuration) {
        this.totalDivCountDuration = totalDivCountDuration;
    }

    public Duration getResponseMinDuration() {
        return responseMinDuration;
    }

    public void setResponseMinDuration(Duration responseMinDuration) {
        this.responseMinDuration = responseMinDuration;
    }

    public Duration getResponseAvgDuration() {
        return responseAvgDuration;
    }

    public void setResponseAvgDuration(Duration responseAvgDuration) {
        this.responseAvgDuration = responseAvgDuration;
    }

    public Duration getResponseMedDuration() {
        return responseMedDuration;
    }

    public void setResponseMedDuration(Duration responseMedDuration) {
        this.responseMedDuration = responseMedDuration;
    }

    public Duration getResponseMaxDuration() {
        return responseMaxDuration;
    }

    public void setResponseMaxDuration(Duration responseMaxDuration) {
        this.responseMaxDuration = responseMaxDuration;
    }

}
