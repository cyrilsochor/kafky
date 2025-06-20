package io.github.cyrilsochor.kafky.api.job;

import java.util.Comparator;

public enum JobState {
    INITIALIZING(false, 0), // creating object without connection to target service

    PREPARING(false, 0), // preparing data without connectoin to target service
    PREPARED(false, 0), // all data are prepared

    STARTING(false, 0), // creating connection to target service
    STARTED(false, 0), // connection to target service is created

    WARMUP(false, 0), // sending warm data to target service
    WARMED(false, 0), //warm data send to target service

    MEASURING_RESPONSE_TIME(false, 0), // sending data to target service and measuring the reponse time 
    MEASURED_RESPONSE_TIME(false, 0), // data sent to target service and the response time was measured

    MEASURING_THROUGHPUT(false, 0), // sending data to target service and measuring the throughput

    OBSERVING(false, 0), // merged states THROUGHPUT-MEASURING_THROUGHPUT for observing jobs (never wait for them)

    CANCELING(false, 0), // received CANCEL request but stil closing the connection to target service
    CANCELED(true, 0), // received CANCEL request and connection to target service is closed, waiting for application exit

    SUCCESS(true, 0), // everything is done without error, waiting for application exit
    FAILED(true, 1), // everything is done with some error, waiting for application exit
    ;

    public static final Comparator<JobState> STATE_COMPARATOR = Comparator.comparing(JobState::ordinal);

    final boolean finite;
    final int exitStatus;

    JobState(boolean finite, int exitStatus) {
        this.finite = finite;
        this.exitStatus = exitStatus;
    }

    public boolean isFinite() {
        return finite;
    }

    public int getExitStatus() {
        return exitStatus;
    }

}